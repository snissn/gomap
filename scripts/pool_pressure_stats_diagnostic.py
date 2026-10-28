#!/usr/bin/env python3
"""Short exact-source public Stats diagnostic; never final storage qualification."""

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


BENCHMARK = "BenchmarkPoolPressurePublicStats"
HARNESS = "TreeDB/caching/pool_pressure_public_stats_bench_test.go"
FIXTURE_VERSION = "public-stats-v2-close-excluded"
PAIRS = 3
BENCHTIME = "250ms"
METRICS = {"ns/op", "B/op", "allocs/op"}
ROW = re.compile(r"^" + BENCHMARK + r"(?:-\d+)?\s+(\d+)\s+(.*)$")
# Shared with the workflow and its source contract test. Other TreeDB changes
# continue to run the existing gates without activating this short diagnostic.
SCOPE = re.compile(r"^(?:TreeDB/caching/(?:db\.go|pool_pressure[^/]*\.go)|"
                   r"scripts/(?:pool_pressure_stats_diagnostic|mvcc_primary_cost_gate)\.py|"
                   r"\.github/scripts/test_pool_pressure_stats_diagnostic\.py)$")


def parse_sample(text: str) -> dict:
    rows = [match for line in text.splitlines() if (match := ROW.fullmatch(line.strip()))]
    if len(rows) != 1 or int(rows[0][1]) <= 0:
        raise ValueError("exactly one completed public Stats sample is required")
    parts = rows[0][2].split()
    metrics = {}
    if len(parts) % 2:
        raise ValueError("malformed benchmark metrics")
    for raw, unit in zip(parts[::2], parts[1::2]):
        value = float(raw)
        if unit not in METRICS or unit in metrics or not math.isfinite(value) or value < 0:
            raise ValueError("unexpected, duplicate, nonfinite or negative metric")
        metrics[unit] = value
    if set(metrics) != METRICS or metrics["ns/op"] <= 0:
        raise ValueError("missing metrics or nonpositive timing")
    if "PASS" not in text.splitlines() or re.search(r"(?m)^(FAIL|--- FAIL:|panic:)", text):
        raise ValueError("benchmark did not complete successfully")
    return {"iterations": int(rows[0][1]), "metrics": metrics}


def evaluate(rows: list[dict]) -> dict:
    expected = [(pair, revision) for pair in range(PAIRS) for revision in
                (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline"))]
    if [(row["pair"], row["revision"]) for row in rows] != expected:
        raise ValueError("missing, duplicated or reordered planned samples")
    values = {revision: [r["metrics"] for r in rows if r["revision"] == revision]
              for revision in ("baseline", "candidate")}
    medians = {revision: {unit: statistics.median(row[unit] for row in samples) for unit in METRICS}
               for revision, samples in values.items()}
    deltas = [(head["ns/op"] / base["ns/op"] - 1) * 100
              for base, head in zip(values["baseline"], values["candidate"])]
    gates = {"timing": statistics.median(deltas) <= maintained.TIME_LIMIT_PERCENT,
             "bytes": medians["candidate"]["B/op"] <= medians["baseline"]["B/op"],
             "allocations": medians["candidate"]["allocs/op"] <= medians["baseline"]["allocs/op"]}
    return {"medians": medians, "paired_deltas_percent": deltas,
            "median_paired_delta_percent": statistics.median(deltas), "gates": gates,
            "measurement_pass": all(gates.values())}


def run(root: Path, output: Path, baseline: str, candidate: str) -> int:
    if not all(re.fullmatch(r"[0-9a-f]{40}", sha) for sha in (baseline, candidate)):
        raise ValueError("full immutable source SHAs are required")
    if baseline == candidate or output.exists() or output == root or root in output.parents:
        raise ValueError("distinct SHAs and a fresh external output directory are required")
    output.mkdir(parents=True)
    (output / "tmp").mkdir()
    environment = dict(os.environ, GOWORK="off", GOMAXPROCS="1", TMPDIR=str(output / "tmp"),
                       PYTHONDONTWRITEBYTECODE="1")
    rows, receipts = [], []
    observation = None
    base_root = None

    def save(name, value):
        (output / name).write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")

    def report(verdict, **details):
        result = dict(details, verdict=verdict, product_guard_available=verdict == "PASS",
                      baseline=baseline, candidate=candidate, retained_samples=len(rows),
                      scope="public Stats diagnostic only; not final storage qualification")
        save("summary.json", result)
        (output / "summary.md").write_text(
            "## Public Stats diagnostic\n\n"
            f"- Verdict: **{verdict}**; product guard available: {result['product_guard_available']}.\n"
            f"- Exact base/head: `{baseline}` / `{candidate}`.\n"
            f"- Three pairs AB/BA/AB, GOMAXPROCS=1, CPU0, {BENCHTIME}; retained rows: {len(rows)}/6.\n"
            f"- Detail: `{json.dumps(details, sort_keys=True)}`\n"
            "- Diagnostic only; NULL/failure supplies no product acceptance. PRIMARY is unchanged.\n")

    def command(label, argv, cwd=root, timeout=120):
        started = time.monotonic()
        stdout, stderr = [output / (label + suffix) for suffix in (".stdout", ".stderr")]
        with stdout.open("wb") as out, stderr.open("wb") as err:
            process = subprocess.Popen([str(x) for x in argv], cwd=cwd, env=environment,
                                       stdout=out, stderr=err, start_new_session=True)
            interrupted = None
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
                interrupted = error
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
                status = 124
        receipts.append(dict(label=label, argv=[str(x) for x in argv], cwd=str(cwd), exit=status,
                             elapsed_s=time.monotonic() - started,
                             stdout=maintained.file_identity(stdout), stderr=maintained.file_identity(stderr)))
        save("commands.json", receipts)
        if interrupted is not None or status:
            raise RuntimeError(f"{label} failed or interrupted (exit {status}); child joined") from interrupted
        return stdout.read_text()

    report("NULL", reason="not yet measured")
    try:
        command("candidate-guard-before", ["bash", root / "scripts/mvcc_candidate_checkout_guard.sh", root, candidate])
        for label, argv in (("go-version", ["go", "version"]), ("go-env", ["go", "env", "-json"]),
                            ("uname", ["uname", "-a"]), ("lscpu", ["lscpu"]),
                            ("processes", ["ps", "-eo", "pid,ppid,etime,pcpu,args"])):
            command(label, argv)
        base_root = Path(tempfile.mkdtemp(prefix="pool-pressure-stats-", dir=output.parent)) / "baseline"
        command("baseline-worktree", ["git", "worktree", "add", "--detach", base_root, baseline])
        harness = output / Path(HARNESS).name
        harness.write_bytes((root / HARNESS).read_bytes())
        save("baseline-overlay.json", {"Replace": {str(base_root / HARNESS): str(harness)}})
        binaries = {}
        for revision, source in (("baseline", base_root), ("candidate", root)):
            binary = output / (revision + "-stats.test")
            argv = ["go", "test", "-c", "-p=1", "-trimpath", "-buildvcs=false"]
            if revision == "baseline":
                argv += ["-overlay", output / "baseline-overlay.json"]
            command("build-" + revision, argv + ["-o", binary, "./TreeDB/caching"], source, timeout=600)
            binaries[revision] = maintained.file_identity(binary)
            command("buildinfo-" + revision, ["go", "version", "-m", binary])
        tracked = [root / HARNESS, Path(__file__), Path(maintained.__file__),
                   root / "scripts/mvcc_candidate_checkout_guard.sh"]
        source_inputs = {str(path.relative_to(root)): maintained.file_identity(path) for path in tracked}
        save("bindings.json", dict(baseline=baseline, candidate=candidate, binaries=binaries,
             fixture_version=FIXTURE_VERSION,
             inputs=source_inputs, pairs=PAIRS, benchtime=BENCHTIME, cpu=0, gomaxprocs=1,
             environment_sha256=hashlib.sha256(json.dumps(environment, sort_keys=True).encode()).hexdigest(),
             thresholds=dict(time_percent=maintained.TIME_LIMIT_PERCENT, median_bytes_growth=0, median_allocations_growth=0),
             quiet=dict(window_s=maintained.QUIET_WINDOW_SECONDS, cap_s=maintained.QUIET_ACQUISITION_SECONDS,
                        external_cpu_percent=maintained.QUIET_LIMIT_PERCENT,
                        aggregate_external_cpu_percent=maintained.QUIET_AGGREGATE_LIMIT_PERCENT,
                        policy="external-cpu-v2")))
        benchmark_argv = ["-test.run=^$", "-test.bench=^" + BENCHMARK + "$", "-test.benchtime=" + BENCHTIME,
                          "-test.count=1", "-test.benchmem", "-test.timeout=60s"]

        def runtime_inputs(phase):
            bindings = {revision: maintained.runtime_binding(Path(record["path"]), command,
                         phase + "-" + revision, environment) for revision, record in binaries.items()}
            for revision, binding in bindings.items():
                if binding["executable"]["sha256"] != binaries[revision]["sha256"]:
                    raise RuntimeError("built binary identity drift")
            save("runtime-inputs-" + phase + ".json", bindings)
            return bindings

        before = runtime_inputs("before")
        maintained.acquire_quiet_window(output)
        observation = maintained.ProcessObservation(output / "measured-processes.jsonl")
        for pair in range(PAIRS):
            for revision in (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")):
                text = command(f"pair-{pair+1}-{revision}", ["taskset", "-c", "0", binaries[revision]["path"]] + benchmark_argv)
                rows.append(dict(pair=pair, revision=revision, **parse_sample(text)))
                save("samples.json", rows)
        observed = observation.finish()
        observation = None
        save("measured-processes.json", observed)
        command("candidate-guard-after", ["bash", root / "scripts/mvcc_candidate_checkout_guard.sh", root, candidate])
        command("baseline-guard-after", ["bash", root / "scripts/mvcc_candidate_checkout_guard.sh", base_root, baseline])
        if (before != runtime_inputs("after") or maintained.digest(harness) != maintained.digest(root / HARNESS)
                or source_inputs != {str(path.relative_to(root)): maintained.file_identity(path) for path in tracked}):
            raise RuntimeError("runtime, binary or source input drift")
        result = evaluate(rows)
        verdict = "NULL" if observed["busy"] else "PASS" if result["measurement_pass"] else "FAIL"
        report(verdict, quiet=not observed["busy"], **result)
        return 0 if verdict == "PASS" else 1
    except Exception as error:
        if observation is not None:
            save("measured-processes.json", observation.finish())
        report("NULL", error=str(error))
        return 1
    finally:
        try:
            if base_root is not None and base_root.exists():
                command("baseline-worktree-remove", ["git", "worktree", "remove", base_root])
            if base_root is not None:
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
