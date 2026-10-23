#!/usr/bin/env python3
"""Frozen #5214 public GetAt/prefix discriminator; never final qualification."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import statistics
import subprocess
import sys
import tempfile
import time

import mvcc_primary_cost_gate as maintained


BASELINE = "8c392a8e27b8f23e795d07edfb3782ca32d46976"
PRODUCT = "f495aa04e9daf1f8cc7ccdb1adb46faf54129b9c"
HARNESS = "TreeDB/mvcc/cow_successor_workspace_external_test.go"
HARNESS_SHA256 = "2226735b88900fe2b33178498172d946de6d536e793f045c5b2f8c1a38544d08"
BENCHMARK = "BenchmarkCOWSuccessorWorkspace"
ITERATIONS, PAIRS, GOMAXPROCS, DEADLINE_SECONDS = 1024, 3, 8, 360
# Coordinator accepted these limits before any timing on 2026-10-23.
# They are specific to this diagnostic, not a revision of PRIMARY policy.
TIME_LIMIT_PERCENT, REQUIRED_SERIAL_ALLOCS_REMOVED = 5.0, 1
METRICS = {"ns/op", "B/op", "allocs/op"}
CELLS = [BENCHMARK + "/" + cell for pointer in ("false", "true") for cell in (
    *[f"GetAt/pointer={pointer}/depth={depth}/concurrency={concurrency}"
      for depth in (1, 8) for concurrency in (1, 8)],
    f"Prefix/pointer={pointer}/depth=8/concurrency=1")]
ORDER = [(pair, revision) for pair in range(PAIRS) for revision in
         (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline"))]
ALLOWED_CHANGES = {
    "scripts/cow_successor_workspace_diagnostic.py",
    ".github/scripts/test_cow_successor_workspace_diagnostic.py",
    ".github/workflows/treedb-tests.yml", ".github/ci/ci_impact.json",
    "TreeDB/docs/benchmarks/cow-successor-workspace-5214.md"}
SCOPE = re.compile(r"^(?:TreeDB/caching/(?:cow_(?:read(?:_workspace)?|cut|successor_workspace_test)|point_successor)\.go|"
                   r"TreeDB/mvcc/cow_successor_workspace_external_test\.go|"
                   r"scripts/(?:cow_successor_workspace_diagnostic|mvcc_primary_cost_gate)\.py|"
                   r"\.github/scripts/test_cow_successor_workspace_diagnostic\.py)$")


def parse_samples(text: str, *, complete=True) -> list[dict]:
    """Accept the exact ordered cells, including Go rows split by setup logging."""
    rows, pending = [], None
    for line in text.splitlines():
        header = re.match(r"^(" + BENCHMARK + r"/\S+?)(?:-\d+)?\s+(.*)$", line.strip())
        if header:
            if pending is not None:
                raise ValueError("new benchmark before preceding row completed")
            pending, line = header.groups()
        if pending is None or not re.match(r"^\s*\d+\s+", line):
            continue
        parts = line.split()
        if int(parts[0]) != ITERATIONS or len(parts[1:]) % 2:
            raise ValueError("wrong fixed iteration count or malformed metrics")
        metrics = {}
        for raw, unit in zip(parts[1::2], parts[2::2]):
            value = float(raw)
            if unit not in METRICS or unit in metrics or not math.isfinite(value) or value < 0:
                raise ValueError("unknown, duplicate, nonfinite or negative metric")
            metrics[unit] = value
        if set(metrics) != METRICS or metrics["ns/op"] <= 0:
            raise ValueError("missing metrics or nonpositive timing")
        rows.append(dict(cell=pending, iterations=ITERATIONS, metrics=metrics,
                         ops_per_second=1e9 / metrics["ns/op"]))
        pending = None
    if [row["cell"] for row in rows] != CELLS[:len(rows)] or len(rows) > len(CELLS):
        raise ValueError("unknown, duplicated or reordered cells")
    if complete and (len(rows) != len(CELLS) or pending is not None or "PASS" not in text.splitlines()
                     or re.search(r"(?m)^(FAIL|--- FAIL:|panic:)", text)):
        raise ValueError("all ten completed successful cells are required")
    return rows


def evaluate(rows: list[dict]) -> dict:
    if [(row["pair"], row["revision"], row["cell"]) for row in rows] != [
            (pair, revision, cell) for pair, revision in ORDER for cell in CELLS]:
        raise ValueError("exactly sixty rows in the predeclared order are required")
    cells = {}
    for cell in CELLS:
        values = {revision: [row["metrics"] for row in rows if row["cell"] == cell
                            and row["revision"] == revision] for revision in ("baseline", "candidate")}
        medians = {revision: {metric: statistics.median(row[metric] for row in samples)
                             for metric in METRICS} for revision, samples in values.items()}
        ratios = [head["ns/op"] / base["ns/op"]
                  for base, head in zip(values["baseline"], values["candidate"])]
        deltas = [(ratio - 1) * 100 for ratio in ratios]
        gates = dict(time=statistics.median(ratios) <= 1 + TIME_LIMIT_PERCENT / 100,
                     bytes=medians["candidate"]["B/op"] <= medians["baseline"]["B/op"],
                     allocations=medians["candidate"]["allocs/op"] <= medians["baseline"]["allocs/op"])
        cells[cell] = dict(medians=medians, paired_time_deltas_percent=deltas,
                           median_paired_time_delta_percent=statistics.median(deltas), gates=gates)
    improvements = {}
    for pointer in ("false", "true"):
        cell = BENCHMARK + f"/GetAt/pointer={pointer}/depth=8/concurrency=1"
        medians = cells[cell]["medians"]
        removed = medians["baseline"]["allocs/op"] - medians["candidate"]["allocs/op"]
        improvements[cell] = dict(median_allocations_removed=removed,
                                  pass_improvement=removed >= REQUIRED_SERIAL_ALLOCS_REMOVED)
    return dict(cells=cells, improvement_gates=improvements,
                measurement_pass=all(all(row["gates"].values()) for row in cells.values())
                and all(row["pass_improvement"] for row in improvements.values()))


def execute(argv, cwd, environment, stdout, stderr, deadline, observation=None, on_start=None):
    """Bound the entire command, kill its process group on interruption and join."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("six-minute diagnostic deadline exhausted")
    with stdout.open("wb") as out, stderr.open("wb") as err:
        process = subprocess.Popen([str(x) for x in argv], cwd=cwd, env=environment,
                                   stdout=out, stderr=err, start_new_session=True)
        try:
            if on_start is not None:
                on_start(process.pid)
            while True:
                if observation is not None:
                    observation.observe()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("six-minute diagnostic deadline exhausted")
                try:
                    return process.wait(timeout=min(0.25, remaining))
                except subprocess.TimeoutExpired:
                    pass
        except BaseException:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            raise


def run(root: Path, output: Path, baseline: str, candidate: str) -> int:
    if baseline != BASELINE or not re.fullmatch(r"[0-9a-f]{40}", candidate):
        raise ValueError("the frozen baseline and a full immutable candidate SHA are required")
    if output.exists() or output == root or root in output.parents:
        raise ValueError("a fresh external artifact directory is required")
    started = time.monotonic()
    deadline = started + DEADLINE_SECONDS
    output.mkdir(parents=True)
    (output / "tmp").mkdir()
    environment = dict(os.environ, GOWORK="off", GOMAXPROCS=str(GOMAXPROCS),
                       TMPDIR=str(output / "tmp"), PYTHONDONTWRITEBYTECODE="1")
    rows, receipts, observation, base_root = [], [], None, None

    def save(name, value):
        (output / name).write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")

    def report(verdict, **details):
        save("summary.json", dict(details, verdict=verdict, product_guard_available=verdict == "PASS",
             final_acceptance=None, baseline=baseline, candidate=candidate, frozen_product=PRODUCT,
             retained_rows=len(rows), expected_rows=60, elapsed_s=time.monotonic() - started))
        description = {key: value for key, value in details.items()
                       if key not in ("cells", "improvement_gates")}
        markdown = (
            "## COW successor public diagnostic (#5214)\n\n"
            f"- Verdict **{verdict}**; rows {len(rows)}/60; no final acceptance.\n"
            f"- Frozen baseline `{baseline}`; tooling head `{candidate}`; product `{PRODUCT}`.\n"
            "- AB/BA/AB; GOMAXPROCS=8; 1024 total calls per leaf. See bindings for actual CPU affinity.\n"
            f"- Detail: `{json.dumps(description, sort_keys=True)}`\n"
            "- Existing PRIMARY and other qualification gates are unchanged.\n")
        if "cells" in details:
            markdown += ("\n| Cell | Base ns/op | Head ns/op | Paired time % | Base B/op | Head B/op | "
                         "Base allocs/op | Head allocs/op | Failed guards |\n|---|---:|---:|---:|---:|---:|---:|---:|---|\n")
            for cell, result in details["cells"].items():
                base, head = (result["medians"][revision] for revision in ("baseline", "candidate"))
                failed = ", ".join(name for name, passed in result["gates"].items() if not passed) or "none"
                markdown += (f"| {cell.removeprefix(BENCHMARK + '/')} | {base['ns/op']} | {head['ns/op']} | "
                             f"{result['median_paired_time_delta_percent']:.3f} | {base['B/op']} | {head['B/op']} | "
                             f"{base['allocs/op']} | {head['allocs/op']} | {failed} |\n")
            for cell, improvement in details["improvement_gates"].items():
                status = "PASS" if improvement["pass_improvement"] else "INSUFFICIENT IMPROVEMENT"
                markdown += f"\n- {cell}: {improvement['median_allocations_removed']} alloc/op removed; {status}.\n"
        (output / "summary.md").write_text(markdown)

    def command(label, argv, cwd=root, *, check=True):
        stdout, stderr = [output / (label + suffix) for suffix in (".stdout", ".stderr")]
        command_started, status, error, child = time.monotonic(), None, None, {}
        def on_start(pid):
            child["pid"] = pid
            try:
                child["allowed_cpus"] = sorted(os.sched_getaffinity(pid))
            except ProcessLookupError:
                child["allowed_cpus"] = None
        try:
            # Reserve a small part of the same deadline for worktree cleanup.
            status = execute(argv, cwd, environment, stdout, stderr, deadline - 5, observation, on_start)
        except BaseException as caught:
            error = caught
        receipts.append(dict(label=label, argv=[str(x) for x in argv], cwd=str(cwd), exit=status,
                             child=child, error=str(error) if error else None,
                             elapsed_s=time.monotonic() - command_started,
                             stdout=maintained.file_identity(stdout) if stdout.exists() else None,
                             stderr=maintained.file_identity(stderr) if stderr.exists() else None))
        save("commands.json", receipts)
        if error is not None:
            raise error
        if check and status:
            raise RuntimeError(f"{label} failed (exit {status})")
        return stdout.read_text(), status

    def checked(label, argv, cwd=root):
        return command(label, argv, cwd)[0]

    def clock():
        if time.monotonic() >= deadline - 5:
            raise TimeoutError("six-minute diagnostic deadline exhausted during quiet acquisition")
        return time.monotonic()

    save("plan.json", dict(baseline=baseline, candidate=candidate, frozen_product=PRODUCT,
         cells=CELLS, order=ORDER, expected_rows=60, iterations=ITERATIONS, gomaxprocs=GOMAXPROCS,
         deadline_s=DEADLINE_SECONDS, policy_accepted_before_timing="2026-10-23 coordinator authorization",
         per_cell_paired_time_percent=TIME_LIMIT_PERCENT, per_cell_median_bytes_growth=0,
         per_cell_median_allocations_growth=0,
         each_serial_depth8_allocations_removed=REQUIRED_SERIAL_ALLOCS_REMOVED))
    save("samples.json", rows)
    save("commands.json", receipts)
    report("NULL", reason="not yet measured")
    try:
        checked("candidate-guard-before", ["bash", root / "scripts/mvcc_candidate_checkout_guard.sh", root, candidate])
        checked("product-ancestor", ["git", "merge-base", "--is-ancestor", PRODUCT, candidate])
        changed = checked("product-diff", ["git", "diff", "--name-only", PRODUCT, candidate]).splitlines()
        if set(changed) - ALLOWED_CHANGES or maintained.digest(root / HARNESS) != HARNESS_SHA256:
            raise ValueError("candidate changed frozen runtime, controls or fixture")
        for label, argv in (("go-version", ["go", "version"]), ("go-env", ["go", "env", "-json"]),
                            ("uname", ["uname", "-a"]), ("lscpu", ["lscpu"]),
                            ("processes", ["ps", "-eo", "pid,ppid,etime,pcpu,args"])):
            checked(label, argv)
        affinity = sorted(os.sched_getaffinity(0))
        save("cpu.json", dict(observer_pid=os.getpid(), allowed_cpus=affinity, allowed_cpu_count=len(affinity),
             gomaxprocs=GOMAXPROCS, oversubscribed=GOMAXPROCS > len(affinity),
             note="Inherited affinity unchanged; concurrency=8 is not an eight-core scaling claim."))
        base_root = Path(tempfile.mkdtemp(prefix="cow-successor-", dir=output.parent)) / "baseline"
        checked("baseline-worktree", ["git", "worktree", "add", "--detach", base_root, baseline])
        harness = output / Path(HARNESS).name
        harness.write_bytes((root / HARNESS).read_bytes())
        save("baseline-overlay.json", {"Replace": {str(base_root / HARNESS): str(harness)}})
        binaries = {}
        for revision, source in (("baseline", base_root), ("candidate", root)):
            binary = output / (revision + "-successor.test")
            argv = ["go", "test", "-c", "-p=1", "-trimpath", "-buildvcs=false"]
            if revision == "baseline":
                argv += ["-overlay", output / "baseline-overlay.json"]
            checked("build-" + revision, argv + ["-o", binary, "./TreeDB/mvcc"], source)
            binaries[revision] = maintained.file_identity(binary)
            checked("buildinfo-" + revision, ["go", "version", "-m", binary])
        tracked = [root / HARNESS, Path(__file__).resolve(), Path(maintained.__file__).resolve(),
                   root / "scripts/mvcc_candidate_checkout_guard.sh"]
        inputs = {str(path.relative_to(root)): maintained.file_identity(path) for path in tracked}
        save("bindings.json", dict(baseline=baseline, candidate=candidate, frozen_product=PRODUCT,
             fixture_sha256=HARNESS_SHA256, inputs=inputs, binaries=binaries,
             cells=CELLS, order=ORDER, iterations=ITERATIONS, gomaxprocs=GOMAXPROCS,
             policy=dict(accepted_before_timing="2026-10-23 coordinator authorization",
                         per_cell_paired_time_percent=TIME_LIMIT_PERCENT,
                         per_cell_median_bytes_growth=0, per_cell_median_allocations_growth=0,
                         each_serial_depth8_allocations_removed=REQUIRED_SERIAL_ALLOCS_REMOVED),
             deadline_s=DEADLINE_SECONDS, environment_sha256=hashlib.sha256(
                 json.dumps(environment, sort_keys=True).encode()).hexdigest(),
             runtime_environment={name: environment.get(name) for name in
                 ("GOMAXPROCS", "GOGC", "GOMEMLIMIT", "GODEBUG", "GOFLAGS", "GOENV", "GOTOOLCHAIN", "GOROOT")},
             quiet=dict(window_s=maintained.QUIET_WINDOW_SECONDS, cap_s=maintained.QUIET_ACQUISITION_SECONDS,
                        external_cpu_percent=maintained.QUIET_LIMIT_PERCENT)))

        def runtime_inputs(phase):
            bindings = {revision: maintained.runtime_binding(Path(record["path"]), checked,
                         phase + "-" + revision, environment) for revision, record in binaries.items()}
            for revision, binding in bindings.items():
                if binding["executable"]["sha256"] != binaries[revision]["sha256"]:
                    raise RuntimeError("built binary identity drift")
            save("runtime-inputs-" + phase + ".json", bindings)
            return bindings

        before = runtime_inputs("before")
        maintained.acquire_quiet_window(output, clock=clock)
        observation = maintained.ProcessObservation(output / "measured-processes.jsonl")
        for pair, revision in ORDER:
            text, status = command(f"pair-{pair+1}-{revision}", [binaries[revision]["path"],
                "-test.run=^$", "-test.bench=^" + BENCHMARK + "$", "-test.benchtime=1024x",
                "-test.count=1", "-test.benchmem"], check=False)
            if receipts[-1]["child"]["allowed_cpus"] != affinity:
                raise RuntimeError("measured child CPU affinity missing or changed")
            rows.extend(dict(pair=pair, revision=revision, **row) for row in parse_samples(text, complete=False))
            save("samples.json", rows)
            if status:
                raise RuntimeError(f"benchmark process failed (exit {status}); partial rows retained")
            parse_samples(text)
        observed = observation.finish()
        observation = None
        save("measured-processes.json", observed)
        checked("candidate-guard-after", ["bash", root / "scripts/mvcc_candidate_checkout_guard.sh", root, candidate])
        checked("baseline-guard-after", ["bash", root / "scripts/mvcc_candidate_checkout_guard.sh", base_root, baseline])
        if (before != runtime_inputs("after") or maintained.digest(harness) != HARNESS_SHA256
                or inputs != {str(path.relative_to(root)): maintained.file_identity(path) for path in tracked}
                or affinity != sorted(os.sched_getaffinity(0))):
            raise RuntimeError("source, fixture, runtime or CPU-affinity drift")
        result = evaluate(rows)
        save("cell-results.json", result)
        verdict = "NULL" if observed["busy"] else "PASS" if result["measurement_pass"] else "FAIL"
        report(verdict, quiet=not observed["busy"], **result)
        return 0 if verdict == "PASS" else 1
    except (Exception, KeyboardInterrupt) as error:
        if observation is not None:
            save("measured-processes.json", observation.finish())
        report("NULL", error=str(error))
        return 1
    finally:
        try:
            if base_root is not None and base_root.exists():
                status = execute(["git", "worktree", "remove", str(base_root)], root, environment,
                                 output / "cleanup.stdout", output / "cleanup.stderr", deadline)
                if status:
                    raise RuntimeError(f"cleanup exit {status}")
            if base_root is not None and base_root.parent.exists():
                base_root.parent.rmdir()
        except Exception as error:
            report("NULL", error=f"cleanup failed: {error}")
            return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--candidate", required=True)
    args = parser.parse_args()
    sys.exit(run(args.root.resolve(), args.output.resolve(), args.baseline, args.candidate))
