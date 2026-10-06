"""Retain the tiny actual public-MVCC diagnostic; no sustained qualification."""
import argparse
import json
import os
from pathlib import Path
import platform
import subprocess
import time

from collect import normalized_identity, now, sha, write_json, validate_benchmark_run, benchmark_pattern


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
    original_controls = {key: environment.pop(key, None) for key in ("GOGC", "GOMEMLIMIT")}
    environment.update(GOWORK="off", GOROOT=go_root, GOMAXPROCS="12",
                       GOCACHE="/mnt/fast4tb/gomap-cow-execution-o2nauuzm/cache",
                       GOMODCACHE="/home/mikers/.gvm/pkgsets/go1.25/global/pkg/mod")
    orders = [["append_only", "btree", "cow_btree"],
              ["cow_btree", "btree", "append_only"],
              ["btree", "append_only", "cow_btree"]]
    policy = {"created_utc": now(), "scope": "tiny C2 public MVCC diagnostic; Store fences retained",
              "source_tree_sha256": identity["source_tree_sha256"],
              "fixture_sha256": sha(source / "TreeDB/mvcc/cow_integration_bench_test.go"),
              "orders": orders, "operations": [128, 256], "logical_keys": 8, "value_bytes": 128,
              "GOMAXPROCS": 12, "original_memory_controls_removed": original_controls,
              "host": platform.uname()._asdict(), "cpu_count": os.cpu_count(),
              "timed_scope": "CommitAt plus GetAt or full exact-key version scan, validation and identical clocks",
              "profile_acknowledgement": "CommitRelaxed uses resolved profile ordinary acknowledgement; no explicit sync promise",
              "memory_scope": "Go B/op timed; process_alloc includes diagnostics; RSS whole child; fixture charges not per-op heap",
              "noise_policy": "retain every sample and repeat spread; no significance or sustained claim",
              "runner_gate": "coordinator must serialize with other owned validations",
              "stream_policy": "separate retained stdout and stderr, each bound by receipt hash"}
    write_json(out / "policy.json", policy)
    with (out / "toolchain.txt").open("w") as stream:
        subprocess.run([go, "version"], env=environment, stdout=stream, check=True)
        subprocess.run([go, "env", "-json"], cwd=source, env=environment, stdout=stream, check=True)
    with (out / "compiled-dependencies.json").open("w") as stream:
        subprocess.run([go, "list", "-deps", "-json", "./TreeDB/mvcc"], cwd=source,
                       env=environment, stdout=stream, check=True)
    binary = out / "mvcc-cost.test"
    with (out / "build.log").open("w") as stream:
        subprocess.run([go, "test", "-c", "-o", str(binary), "./TreeDB/mvcc"], cwd=source,
                       env=environment, stdout=stream, stderr=subprocess.STDOUT, check=True)
    with (out / "binary-buildinfo.txt").open("w") as stream:
        subprocess.run([go, "version", "-m", str(binary)], env=environment, stdout=stream, check=True)
    write_json(out / "binary.json", {"sha256": sha(binary), "bytes": binary.stat().st_size})
    receipts = []
    for repeat, order in enumerate(orders, 1):
        for mode in order:
            for count in (128, 256):
                label = f"r{repeat}-{mode}-N{count}"
                (out / f"{label}-processes-before.txt").write_text(subprocess.check_output(
                    ["ps", "-eo", "pid,ppid,etimes,pcpu,rss,args", "--no-headers"], text=True))
                command = [str(binary), "-test.run=^$",
                           "-test.bench=" + benchmark_pattern("mvcc", mode),
                           f"-test.benchtime={count}x", "-test.count=1", "-test.timeout=300s"]
                start, clock, load = now(), time.monotonic(), os.getloadavg()
                log = out / f"{label}.log"
                stderr = out / f"{label}.stderr.log"
                with log.open("w") as stream, stderr.open("w") as errors:
                    child = subprocess.Popen(command, cwd=source, env=environment,
                                             stdout=stream, stderr=errors)
                    _, status, usage = os.wait4(child.pid, 0)
                    child.returncode = os.waitstatus_to_exitcode(status)
                validation, validation_error = None, None
                if child.returncode == 0:
                    try:
                        validation = validate_benchmark_run(log, stderr, "mvcc", mode, None, count)
                    except (ValueError, OSError) as error:
                        validation_error = str(error)
                receipt = {"label": label, "started_utc": start, "completed_utc": now(),
                           "command": command, "exit_code": child.returncode,
                           "elapsed_seconds": time.monotonic() - clock, "load_before": load,
                           "user_seconds": usage.ru_utime, "system_seconds": usage.ru_stime,
                           "child_max_rss_kib": usage.ru_maxrss, "log_sha256": sha(log),
                           "stderr_sha256": sha(stderr),
                           "case_validation": validation, "validation_error": validation_error,
                           "source_drift_after": drift()}
                receipt["load_after"] = os.getloadavg()
                (out / f"{label}-processes-after.txt").write_text(subprocess.check_output(["ps", "-eo", "pid,ppid,etimes,pcpu,rss,args", "--no-headers"], text=True))
                receipts.append(receipt)
                write_json(out / "receipts.json", receipts)
                print(json.dumps(receipt), flush=True)
                if child.returncode or validation_error or receipt["source_drift_after"]:
                    raise SystemExit("retained failed run; fix and freeze before recollection")
    write_json(out / "completion.json", {"completed_utc": now(), "source_drift": drift(),
                                           "runs": len(receipts), "qualification": "not C3/C4 acceptance"})


if __name__ == "__main__":
    main()
