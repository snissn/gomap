"""Collect a bounded, exact-source C2 diagnostic after writer releases the runner.

Not a sustained C4 qualification. Invoke on Linux185 with an immutable source,
the complete source manifest and a new owned output directory. All raw runs,
including failures and contaminated runs, remain in the output packet.
"""
import argparse
import datetime
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import time


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def normalized_identity(identity):
    """Accept the writer's commit archive manifest without losing its identity."""
    result = dict(identity)
    files = result["files"]
    if isinstance(files, dict):
        result["files"] = [{"path": path, "sha256": digest}
                           for path, digest in sorted(files.items())]
    if len({item["path"] for item in result["files"]}) != len(result["files"]):
        raise ValueError("duplicate source path in manifest")
    if "source_tree_sha256" not in result:
        encoded = "".join(f'{item["sha256"]}  {item["path"]}\n'
                          for item in sorted(result["files"], key=lambda x: x["path"]))
        result["source_tree_sha256"] = hashlib.sha256(encoded.encode()).hexdigest()
        result["source_tree_sha256_derivation"] = "sha256(sorted sha256 + two spaces + relative path + newline)"
    return result


def benchmark_pattern(kind, mode, operation=None):
    if mode not in ("append_only", "btree", "cow_btree"):
        raise ValueError("unknown mode")
    profiles = "^(command_wal_durable|command_wal_relaxed|no_wal_fast)$"
    if kind == "rawkv":
        if operation not in ("FreshCapture", "(CaptureReadRelease|Forward16|IncrementalWrite)",
                             "(DirtyCheckpoint|IncrementalWriteSync)"):
            raise ValueError("unknown operation group")
        return f"^BenchmarkCOWPublicDirtyCost$/{profiles}/^{mode}$/^(Inline64|Pointer4096)$/^N=(1024|2048)$/^{operation}$"
    if kind == "mvcc":
        return f"^BenchmarkCOWIntegratedPublicMVCC$/{profiles}/^{mode}$/^(point|all_versions)$"
    raise ValueError("unknown benchmark packet kind")


def validate_benchmark_run(log, stderr, kind, mode, group, count):
    """Reject filter overlap, missing rows and broken output before another run."""
    text = log.read_text()
    if not re.search(r"^PASS$", text, re.M):
        raise ValueError("missing package PASS")
    if re.search(r"WARNING: DATA RACE|fatal error:", stderr.read_text()):
        raise ValueError("runtime failure in stderr")
    if kind == "rawkv":
        from analyze import PROFILES, LAYOUTS, GROUP_OPERATIONS, parse_row
        expected = set(itertools.product(PROFILES, (mode,), LAYOUTS,
                                         (1024, 2048), GROUP_OPERATIONS[group]))
        fields = ("profile", "mode", "layout", "records", "operation")
    elif kind == "mvcc":
        from analyze_mvcc import PROFILES, parse_row
        expected = set(itertools.product(PROFILES, (mode,),
                                         ("point", "all_versions"), (count,)))
        fields = ("profile", "mode", "read", "iterations")
    else:
        raise ValueError("unknown benchmark packet kind")
    rows = [row for line in text.splitlines()
            if (row := parse_row(line)) is not None]
    actual = {tuple(row[field] for field in fields) for row in rows}
    if actual != expected or len(rows) != len(expected):
        raise ValueError("missing, duplicate or extra benchmark cases")
    if any(row["iterations"] != count for row in rows):
        raise ValueError("unexpected operation count")
    return {"rows": len(rows), "exact_case_set": True, "iterations": count}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--identity", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    source, out = args.source.resolve(), args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    identity = normalized_identity(json.loads(args.identity.read_text()))
    write_json(out / "source-identity.json", identity)

    def drift():
        expected = {item["path"] for item in identity["files"]}
        changed = [item["path"] for item in identity["files"]
                   if not (source / item["path"]).is_file()
                   or sha(source / item["path"]) != item["sha256"]]
        actual = {path.relative_to(source).as_posix() for path in source.rglob("*")
                  if path.is_file() or path.is_symlink()}
        return changed + ["EXTRA:" + path for path in sorted(actual - expected)]

    before = drift()
    write_json(out / "hydration.json", {"at": now(), "drift": before})
    if before:
        raise SystemExit("source does not match manifest")

    go_root = "/home/mikers/.gvm/gos/go1.26.3"
    go = go_root + "/bin/go"
    environment = os.environ.copy()
    original_memory_controls = {key: environment.get(key) for key in ("GOGC", "GOMEMLIMIT")}
    for key in original_memory_controls:
        environment.pop(key, None)
    environment.update(GOWORK="off", GOROOT=go_root, GOMAXPROCS="12",
                       GOCACHE="/mnt/fast4tb/gomap-cow-execution-o2nauuzm/cache",
                       GOMODCACHE="/home/mikers/.gvm/pkgsets/go1.25/global/pkg/mod")
    policy = {
        "created_utc": now(), "scope": "bounded C2 integration diagnostic only",
        "source_tree_sha256": identity["source_tree_sha256"],
        "fixture_sha256": sha(source / "TreeDB/cow_public_cost_bench_test.go"),
        "GOMAXPROCS": 12, "original_memory_controls_removed": original_memory_controls,
        "orders": [["append_only", "btree", "cow_btree"],
                   ["cow_btree", "btree", "append_only"],
                   ["btree", "append_only", "cow_btree"]],
        "FreshCapture_operations": 16, "other_operations": 128,
        "durability_operations": 16,
        "fixtures": "three profiles, Inline64/Pointer4096, N1024/N2048",
        "noise_policy": "retain every run; describe repeat spread; no significance or sustained claim",
        "allocation_scope": "testing timed B/op and allocs/op; caller setup excluded",
        "rss_scope": "maximum child RSS includes fixture construction, all cases and teardown",
        "host": platform.uname()._asdict(), "cpu_count": os.cpu_count(),
        "runner_gate": "coordinator must serialize this collection with other owned validations",
        "stream_policy": "separate retained stdout and stderr, each bound by receipt hash",
    }
    write_json(out / "policy.json", policy)
    with (out / "toolchain.txt").open("w") as stream:
        subprocess.run([go, "version"], env=environment, stdout=stream, check=True)
        subprocess.run([go, "env", "-json"], cwd=source, env=environment,
                       stdout=stream, check=True)
    with (out / "compiled-dependencies.json").open("w") as stream:
        subprocess.run([go, "list", "-deps", "-json", "./TreeDB"], cwd=source,
                       env=environment, stdout=stream, check=True)
    binary = out / "treedb-cost.test"
    with (out / "build.log").open("w") as stream:
        subprocess.run([go, "test", "-c", "-o", str(binary), "./TreeDB"], cwd=source,
                       env=environment, stdout=stream, stderr=subprocess.STDOUT, check=True)
    with (out / "binary-buildinfo.txt").open("w") as stream:
        subprocess.run([go, "version", "-m", str(binary)], env=environment,
                       stdout=stream, check=True)
    write_json(out / "binary.json", {"sha256": sha(binary), "bytes": binary.stat().st_size})

    receipts = []
    for repeat, order in enumerate(policy["orders"], 1):
        for mode in order:
            for group, operation, count in [
                    ("fresh", "FreshCapture", 16),
                    ("other", "(CaptureReadRelease|Forward16|IncrementalWrite)", 128),
                    ("durability", "(DirtyCheckpoint|IncrementalWriteSync)", 16)]:
                label = f"r{repeat}-{mode}-{group}"
                processes = subprocess.check_output(
                    ["ps", "-eo", "pid,ppid,etimes,pcpu,rss,args", "--no-headers"], text=True)
                (out / f"{label}-processes-before.txt").write_text(processes)
                command = [str(binary), "-test.run=^$",
                           "-test.bench=" + benchmark_pattern("rawkv", mode, operation),
                           f"-test.benchtime={count}x", "-test.count=1", "-test.timeout=300s"]
                start, load = now(), os.getloadavg()
                clock = time.monotonic()
                log = out / f"{label}.log"
                stderr = out / f"{label}.stderr.log"
                with log.open("w") as stream, stderr.open("w") as errors:
                    result = subprocess.Popen(command, cwd=source, env=environment,
                                              stdout=stream, stderr=errors)
                    _, status, usage = os.wait4(result.pid, 0)
                    result.returncode = os.waitstatus_to_exitcode(status)
                elapsed = time.monotonic() - clock
                validation, validation_error = None, None
                if result.returncode == 0:
                    try:
                        validation = validate_benchmark_run(log, stderr, "rawkv", mode, group, count)
                    except (ValueError, OSError) as error:
                        validation_error = str(error)
                receipt = {"label": label, "started_utc": start, "completed_utc": now(),
                           "command": command, "exit_code": result.returncode,
                           "elapsed_seconds": elapsed, "load_before": load,
                           "user_seconds": usage.ru_utime,
                           "system_seconds": usage.ru_stime,
                           "child_max_rss_kib": usage.ru_maxrss,
                           "log_sha256": sha(log), "stderr_sha256": sha(stderr),
                           "case_validation": validation, "validation_error": validation_error,
                           "source_drift_after": drift()}
                receipts.append(receipt)
                write_json(out / "receipts.json", receipts)
                print(json.dumps(receipt), flush=True)
                if result.returncode or validation_error or receipt["source_drift_after"]:
                    raise SystemExit("retained failed run; fix and freeze before recollection")
    write_json(out / "completion.json", {"completed_utc": now(), "source_drift": drift(),
                                           "runs": len(receipts), "qualification": "not C4 acceptance"})


if __name__ == "__main__":
    main()
