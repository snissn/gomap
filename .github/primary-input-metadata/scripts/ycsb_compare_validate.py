#!/usr/bin/env python3
"""Fail-closed consumer for go-ycsb close receipt v1 (snissn/go-ycsb#7).

runtime.json and lifecycle.json aggregate original live observations and calls.
Mandatory contract2 checks the original raw evidence and typed snapshots. See
scripts/ycsb_compare_receipts.md for the observation contract.
"""
import argparse
import csv
import contextlib
import datetime as dt
import hashlib
import json
import math
import os
import pathlib
import platform
import re
import signal
import select
import subprocess
import sys
import time
import uuid

from ycsb_evidence_json import read, same
RUNTIME_HELPER_SHA = "9ea6f525e0cf46df5301be3f903367816bdb032a3d0abb4c204bda60ee104d8f"
if hashlib.sha256((pathlib.Path(__file__).resolve().parent / "ycsb_runtime_observer.py").read_bytes()).hexdigest() != RUNTIME_HELPER_SHA:
    raise RuntimeError("runtime observer source hash mismatch")
import ycsb_runtime_observer as runtime_observer

PRODUCER = "3c8148495b8d4490cf0ec30e9eb92d14a7a71bac"
CONTROLLED_CLIENT_SHA = "7dd046a58f9a3deb2560a55bf9f6b2cd5e167787aace0469e0fa671cade9a00e"
CONTROLLED_GOMAX_SYMBOL = 0x4723c10
REFERENCE_CLIENT_SHA = "2237d8aeec309b9b4b335b348234d9829594c9101043144a944d9f2868fbc95b"
OBSERVED_CLIENT_SYMBOLS = {CONTROLLED_CLIENT_SHA: CONTROLLED_GOMAX_SYMBOL, REFERENCE_CLIENT_SHA: 0x4715c10}
RUNTIME_SAMPLE_SECONDS = 0.25
TARGETS = ("treedb_native", "treedb_mongo", "mongodb")
NUM = r"[0-9]+(?:\.[0-9]+)?"
ROW = re.compile(r"^([A-Z_]+)\s+- Takes\(s\): (" + NUM + r"), Count: ([0-9]+), OPS: (" + NUM + r"), Avg\(us\): (" + NUM + r"), Min\(us\): (" + NUM + r"), Max\(us\): (" + NUM + r"), 50th\(us\): (" + NUM + r"), 90th\(us\): (" + NUM + r"), 95th\(us\): (" + NUM + r"), 99th\(us\): (" + NUM + r"), 99.9th\(us\): (" + NUM + r"), 99.99th\(us\): (" + NUM + r")$")
SHA = re.compile(r"[0-9a-f]{64}$")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def schema(value):
    return isinstance(value,dict) and type(value.get("schema_version")) is int and value["schema_version"] == 1


def digest(path):
    with open(path, "rb") as stream:
        value = hashlib.sha256()
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
        return value.hexdigest()


def write_new(path, value):
    with open(path, "x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write("\n")


def timestamp(value):
    """Exact UTC nanoseconds; chronology never rounds through float seconds."""
    require(isinstance(value,str), "invalid UTC timestamp")
    match=re.fullmatch(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.([0-9]{1,9}))?Z",value)
    require(match is not None and not value.startswith("0001"), "invalid UTC timestamp")
    wall=dt.datetime.fromisoformat(match[1]).replace(tzinfo=dt.timezone.utc)
    delta=wall-dt.datetime(1970,1,1,tzinfo=dt.timezone.utc)
    return (delta.days*86400+delta.seconds)*1_000_000_000+int((match[2] or "").ljust(9,"0"))


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def result(value, label, timed=True, bounds=None):
    require(isinstance(value, dict), f"missing {label}")
    require(value.get("attempted") is True and value.get("success") is True and value.get("error") == "", f"failed/skipped {label}")
    elapsed = value["elapsed_ns"]
    require(type(elapsed) is int and elapsed >= 0, f"invalid {label} duration")
    if not timed:
        require(value["start_utc"] == value["end_utc"] == "0001-01-01T00:00:00Z" and elapsed == 0, f"invalid untimed {label}")
    if timed:
        start, end = timestamp(value["start_utc"]), timestamp(value["end_utc"])
        require(start <= end, f"invalid {label} duration")
        require(abs(end-start-elapsed) <= 2_000_000, f"inconsistent {label} duration")
        if bounds:
            require(bounds[0] <= start <= end <= bounds[1], f"stale/out-of-order {label}")
        return start, end


def process(value):
    require(isinstance(value,dict), "missing process object")
    require(type(value.get("pid")) is int and value["pid"] > 0, "missing child PID")
    require(type(value.get("start_ticks")) is int and value["start_ticks"] > 0, "missing child start identity")
    require(SHA.fullmatch(value.get("exe_sha256", "")), "missing observed ELF hash")
    require(value.get("gomaxprocs_unset") is True, "child GOMAXPROCS was not unset")


def exit_success(value,label):
    require(type(value) is int and value == 0,label)


def client_build(value):
    require(schema(value), "unsupported client build schema")
    require(value["producer_commit"] == PRODUCER and isinstance(value["source_commit"],str) and re.fullmatch(r"[0-9a-f]{40}",value["source_commit"]), "unbound producer source")
    for key in ("exe_sha256","source_patch_sha256"):
        require(isinstance(value[key],str) and SHA.fullmatch(value[key]),"missing client build hash")
    require(all(isinstance(value[key],str) and value[key] for key in ("build_id","toolchain","compatibility_configuration")),"incomplete client build binding")


def server_build(value,target):
    require(schema(value) and isinstance(value["version"],str) and value["version"],"missing server version binding")
    require(isinstance(value["source_commit"],str) and re.fullmatch(r"[0-9a-f]{40}",value["source_commit"]),"missing server source binding")
    require(all(isinstance(value[key],str) and SHA.fullmatch(value[key]) for key in ("exe_sha256","source_patch_sha256")),"missing server source/ELF hash")
    require(value["expected_owner"] in ({"mongodb"} if target=="mongodb" else {"legacy","cow"}),"invalid expected owner")


def observation(value,build,profile):
    require(schema(value) and value["kind"]=="live_owner_observation","missing raw live owner observation")
    process(value["process"])
    require(isinstance(value["data_dir"],str) and pathlib.Path(value["data_dir"]).is_absolute(),"missing canonical data directory")
    require(type(value["data_device"]) is int and value["data_device"]>=0 and type(value["data_inode"]) is int and value["data_inode"]>0,"missing data directory inode")
    require(isinstance(value["owner_id"],str) and re.fullmatch(r"[0-9a-f]{32}",value["owner_id"]),"missing actual owner token")
    require(type(value["observed_gomaxprocs"]) is int and value["observed_gomaxprocs"]>0,"missing observed runtime GOMAXPROCS")
    stats=value["stats"]
    require(isinstance(stats,dict) and all(isinstance(k,str) and isinstance(v,str) for k,v in stats.items()),"owner stats must map strings to strings")
    require(value["profile"]==profile and stats["treedb.profile.resolved"]==profile,"actual owner profile mismatch")
    require(value["backend"]==build["expected_owner"],"actual raw backend mismatch")
    if build["expected_owner"]=="cow":
        require(value["capture_error"]=="" and stats["treedb.cache.memtable_mode"]=="cow_btree","missing actual COW capture/cache evidence")
        seq=stats["treedb.cache.cow.txn.logical_seq"]
        require(re.fullmatch(r"[0-9]+",seq) is not None and int(seq)<1<<64,"missing actual COW sequence")
    else:
        require(build["expected_owner"]=="legacy" and value["capture_error"]=="treedb: command kind has no native semantic preparation","missing actual legacy observation")
    timestamp(value["observed_utc"])


def parse_log(path):
    # The pinned go-ycsb emits cumulative progress summaries from a ticker.
    # Its final summary is emitted only after the ticker goroutine has stopped,
    # following the unique "Run finished" marker.  Progress is not another
    # completed sample; do not choose a row by occurrence or overwrite it.
    rows = {}
    text = path.read_text(encoding="utf-8")
    require(text.endswith("\n"), f"truncated output in {path.name}")
    finished = False
    progress = {}
    for line in text.splitlines():
        if line.lstrip().startswith("Run finished"):
            require(re.fullmatch(r"Run finished, takes (?:[0-9]+(?:\.[0-9]+)?(?:h|ms|m|s|µs|us|ns))+", line) is not None, f"malformed final marker in {path.name}")
            require(not finished, f"duplicate final marker in {path.name}")
            finished = True
            continue
        match = ROW.fullmatch(line)
        if not match:
            require(not re.match(r"^\s*[A-Z_]+\s+-", line) and " - Takes" not in line and "Takes(s):" not in line, f"malformed measurement row in {path.name}")
            continue
        op, takes, count, ops, avg, minimum, maximum, p50, p90, p95, p99, p999, p9999 = match.groups()
        # A nonzero error seen during progress cannot be made valid by a
        # later missing or inconsistent final error row.
        if not finished:
            require(not op.endswith("_ERROR") or int(count) == 0, f"operation errors in progress in {path.name}")
            require(int(count) >= progress.get(op, 0), f"decreasing progress count in {path.name}")
            progress[op] = int(count)
            continue
        require(all(math.isfinite(float(n)) for n in (takes,ops,avg,minimum,maximum,p50,p90,p95,p99,p999,p9999)), f"invalid nonfinite measurement in {path.name}")
        require(op not in rows, f"duplicate {op} row in {path.name}")
        rows[op] = dict(operation=op, count=int(count), ops_per_sec=float(ops), avg_us=float(avg), p50_us=float(p50), p95_us=float(p95), p99_us=float(p99), p999_us=float(p999), max_us=float(maximum))
    require(finished, f"missing final marker in {path.name}")
    require(all(op in rows and rows[op]["count"] >= count for op, count in progress.items()), f"final rows do not cover progress in {path.name}")
    return list(rows.values())


def check_counts(parsed, expected, phase, path):
    rows = {row["operation"]: row for row in parsed}
    require("TOTAL" in rows, f"missing TOTAL in {path.name}")
    allowed = {"INSERT", "INSERT_ERROR", "TOTAL"} if phase == "load" else {"READ", "READ_ERROR", "UPDATE", "UPDATE_ERROR", "TOTAL"}
    require(set(rows) <= allowed, f"unexpected workload operations in {path.name}")
    required = {"INSERT"} if phase == "load" else {"READ", "UPDATE"}
    require(required <= rows.keys(), f"missing required measured operations in {path.name}")
    require(all(rows[op]["count"] > 0 for op in required), f"empty required measured operation in {path.name}")
    require(rows["TOTAL"]["count"] == expected, f"TOTAL count mismatch in {path.name}")
    successful = [row for op, row in rows.items() if op != "TOTAL" and not op.endswith("_ERROR")]
    require(successful and sum(row["count"] for row in successful) == expected, f"operation count mismatch in {path.name}")
    require(all(row["count"] == 0 for op, row in rows.items() if op.endswith("_ERROR")), f"operation errors in {path.name}")


def phase_evidence(directory, stem, target, manifest):
    if "common_load" in manifest:
        import ycsb_public_sentinel as sentinel
        sentinel.validate_phase(directory,stem,manifest)
    launch = read(directory / f"{stem}.launch.json")
    receipt_path = directory / f"{stem}.close.json"
    receipt = read(receipt_path)
    require(schema(launch), "unsupported phase launch schema")
    process(launch["process"])
    require(same_timeout(launch["timeout_seconds"],manifest["deadlines"]["phase_seconds"]) and launch["timed_out"] is False and same_timeout(launch["cleanup_timeout_seconds"],manifest["deadlines"]["cleanup_seconds"]) and same_timeout(launch["outer_timeout_seconds"],manifest["deadlines"]["phase_outer_seconds"]), "phase deadline binding mismatch")
    client = manifest["client"]
    require(launch["process"]["exe_sha256"] == client["exe_sha256"], "client ELF mismatch")
    if manifest.get("require_client_runtime_observation") is True or launch.get("runtime_observation") is not None:
        validate_client_runtime(launch.get("runtime_observation"),launch["process"])
    exit_success(launch["exit_code"],"client exit failure or launcher timeout/failure")
    require(launch.get("timed_out") is not True and not (directory/f"{stem}.failure.json").exists(), "client exit failure or launcher timeout/failure")
    require(launch["output_sha256"] == digest(directory / f"{stem}.out"), "stdout hash mismatch")
    require(launch["receipt_sha256"] == digest(receipt_path), "receipt hash mismatch")
    require(launch["campaign_id"] == manifest["campaign_id"], "stale launcher campaign")
    require(schema(receipt) and receipt["total_excludes_close"] is True, "unsupported close receipt")
    require(receipt["phase_id"] == launch["phase_id"] and receipt["client_build_id"] == client["build_id"], "phase/build identity mismatch")
    phase = "load" if stem == "load" else "run"
    require(receipt["phase"] == phase and receipt["db"] == ("treedb-native" if target == "treedb_native" else "mongodb"), "DB/phase mismatch")
    require(receipt["declared_profile"] == manifest["profile"], "profile mismatch")
    bounds = timestamp(launch["start_utc"]), timestamp(launch["end_utc"])
    require(bounds[0] <= bounds[1], "invalid launcher times")
    result(receipt["command"], "command", timed=False)  # producer command result has zero timestamps
    db_bounds = result(receipt["db_close"], "DB Close", bounds=bounds)
    result(receipt["workload_close"], "workload Close", bounds=(db_bounds[1], bounds[1]))
    native = receipt["native_flush"]
    if target == "treedb_native":
        require(native["applicable"] is True and native["flush_enabled"] is True and type(native["ack"]) is int and native["ack"] == 1, "required native flush/requested visible ACK missing")
        dial = result(native["dial"], "native dial", bounds=db_bounds)
        flush = result(native["flush"], "native FlushAll", bounds=(dial[1], db_bounds[1]))
        result(native["connection_close"], "native connection Close", bounds=(flush[1], db_bounds[1]))
    else:
        require(native["applicable"] is False and native["flush_enabled"] is False, "unexpected native flush")
    return launch["phase_id"]


def server_evidence(directory, target, manifest, phase_ids):
    if "common_load" in manifest:
        import ycsb_public_sentinel as sentinel
        sentinel.validate(directory,manifest,read(directory/"server.launch.json")["process"],manifest["common_load"]["address"])
    require(not list(directory.glob("*.failure.json")), "retained launcher/helper failure invalidates target")
    runtime = read(directory / "runtime.json")
    lifecycle = read(directory / "lifecycle.json")
    launch = read(directory / "server.launch.json")
    process(launch["process"])
    if manifest.get("require_client_runtime_observation") is True or any(
        "cpu_policy" in read(path) for prefix in ("server", "reopened", "fixture-write", "fixture-reopened")
        if (path := directory / f"{prefix}.launch.json").exists()
    ):
        validate_server_policies(directory)
    build = manifest["server_builds"][target]
    server_manifest = read(directory.parent / "server-build.json")
    require(schema(server_manifest), "server build manifest mismatch")
    server_build(build,target)
    server_build(server_manifest["servers"][target],target)
    require(same(server_manifest["servers"][target],build), "server build manifest mismatch")
    require(schema(build) and isinstance(build["version"], str) and build["version"], "missing server version binding")
    require(re.fullmatch(r"[0-9a-f]{40}", build["source_commit"]), "missing server source binding")
    require(SHA.fullmatch(build["exe_sha256"]) and launch["process"]["exe_sha256"] == build["exe_sha256"], "server ELF binding mismatch")
    require(build["expected_owner"] in ({"mongodb"} if target == "mongodb" else {"legacy", "cow"}), "invalid expected owner")
    for evidence in (runtime, lifecycle):
        require(evidence["version"] == build["version"] and evidence["source_commit"] == build["source_commit"], "server version/source mismatch")
        require(schema(evidence) and evidence["campaign_id"] == manifest["campaign_id"], "stale server evidence")
        require(evidence["target"] == target and evidence["profile"] == manifest["profile"] and evidence["format"] == manifest["formats"][target], "server settings mismatch")
        process(evidence["process"])
        require(same(evidence["process"],launch["process"]), "server process identity mismatch")
        require(evidence["data_dir"] == launch["data_dir"] and pathlib.Path(evidence["data_dir"]).is_absolute(), "data directory mismatch")
        require(evidence["owner_id"] and evidence["owner_id"] == runtime["owner_id"], "owner mismatch")
    require(same(runtime["phase_ids"],phase_ids), "runtime phase identity mismatch")
    require(type(runtime["observed_gomaxprocs"]) is int and runtime["observed_gomaxprocs"] > 0, "missing actual runtime GOMAXPROCS")
    require(runtime["backend"] == build["expected_owner"], "actual owner/backend does not match selected version")
    require(isinstance(runtime["observation"], str) and runtime["observation"], "missing runtime witness provenance")
    require(same(lifecycle["phase_ids"],phase_ids), "lifecycle phase identity mismatch")
    exit_success(lifecycle["server_exit_code"], "server close exit failure")
    settled = max(timestamp(read(directory/(stem+".launch.json"))["end_utc"]) for stem in ["load"]+[f"run_{i}" for i in range(1,manifest["run_repeats"]+1)])
    checkpoint = result(lifecycle["checkpoint"], "checkpoint", bounds=(settled, float("inf")))
    closed = result(lifecycle["server_close"], "server_close", bounds=(checkpoint[1], float("inf")))
    reopened = result(lifecycle["reopen"], "reopen", bounds=(closed[1], float("inf")))
    preserved = result(lifecycle["random_state_preservation"], "random_state_preservation", bounds=(reopened[1], float("inf")))
    require(settled <= timestamp(lifecycle["preclose_state_utc"]) <= closed[0], "preclose snapshot timing mismatch")
    require(reopened[1] <= timestamp(lifecycle["reopened_state_utc"]) <= preserved[1], "reopened snapshot timing mismatch")
    before, after = lifecycle["preclose_state_sha256"], lifecycle["reopened_state_sha256"]
    require(SHA.fullmatch(before) and before == after, "random state preservation mismatch")
    require(lifecycle["random_state_scope"] == "observed-state-preservation-only", "random-state scope missing")
    stopped = read(directory / "server.exit.json")
    process(stopped["process"])
    exit_success(stopped["exit_code"],"owned service cleanup failed")
    require(same(stopped["process"],launch["process"]), "owned service cleanup failed")
    import ycsb_compare_live
    return ycsb_compare_live.validate(directory,target,manifest,lifecycle)


def validate(out):
    manifest = read(out / "manifest.json")
    if manifest.get("kind") == "mongodb-reference-checkpoint-close-reopen":
        # A separate contract: MongoDB has Docker/public-command observations,
        # never TreeDB runtime, COW or original-owner Close return receipts.
        import ycsb_mongodb_reference
        report = ycsb_mongodb_reference.validate(out)
        (out / "validation.json").write_text(json.dumps(report,indent=2,allow_nan=False)+"\n")
        (out / "summary.md").write_text("# MongoDB reference lifecycle\n\nStatus: "+report["status"]+"\n\nSeparate reference evidence; no TreeDB baseline, COW, observed Close return, crash recovery or performance matrix qualification.\n")
        return 0
    require(schema(manifest) and manifest["campaign_id"], "missing manifest identity")
    require(manifest["profile"] in {"command_wal_durable", "command_wal_relaxed", "no_wal_fast"}, "unsupported profile")
    for key in ("recordcount", "operationcount", "threadcount", "run_repeats"):
        require(type(manifest[key]) is int and manifest[key] > 0, f"invalid {key}")
    client = manifest["client"]
    client_build(client)
    copied_client=read(out/"client-build.json")
    client_build(copied_client)
    import ycsb_native_client_build
    ycsb_native_client_build.comparison(out,manifest)
    require(same(client,copied_client), "client build manifest mismatch")
    require(client["source_patch_sha256"] == digest(out / "client.patch"), "client patch mismatch")
    require((out / "client-buildinfo.txt").read_text().strip(), "missing actual client build info")
    require(SHA.fullmatch(manifest["client"]["exe_sha256"]), "missing client build hash")
    require(manifest["targets"] and len(set(manifest["targets"])) == len(manifest["targets"]) and set(manifest["targets"]) <= set(TARGETS), "invalid targets")
    import ycsb_compare_live
    phase_stats = ycsb_compare_live.phase_stats_enabled(manifest)
    for key in ("phase_seconds","helper_seconds","cleanup_seconds","phase_outer_seconds","helper_outer_seconds"):
        positive_timeout(manifest["deadlines"][key],f"manifest {key}")
    rows, errors, seen_ids = [], [], set()
    phase_counter_rows = {}
    measured_intervals=[]
    for target in manifest["targets"]:
        require(manifest["formats"][target] in {"json", "bson"}, "unsupported document format")
        directory = out / target
        ids = []
        for stem in ["load"] + [f"run_{i}" for i in range(1, manifest["run_repeats"]+1)]:
            try:
                phase = "load" if stem == "load" else "run"
                log_path = directory / f"{stem}.out"
                parsed = parse_log(log_path)
                operation_errors = sum(row["count"] for row in parsed if row["operation"].endswith("_ERROR"))
                rows.extend(dict(target=target, phase=phase, repeat=0 if phase == "load" else int(stem[4:]), operation_error_count=operation_errors, **row) for row in parsed)
                check_counts(parsed, manifest["recordcount"] if phase == "load" else manifest["operationcount"], phase, log_path)
                phase_id = phase_evidence(directory, stem, target, manifest)
                require(phase_id not in seen_ids, "reused phase identity")
                seen_ids.add(phase_id)
                ids.append(phase_id)
                measured=read(directory/(stem+".launch.json"))
                measured_intervals.append((target+"/"+stem,timestamp(measured["start_utc"]),timestamp(measured["end_utc"])))
            except (ValueError, KeyError, TypeError, OSError) as error:
                errors.append(f"{target}/{stem}: {error}")
        try:
            observed_phases = server_evidence(directory, target, manifest, ids)
            if phase_stats:
                phase_counter_rows[target] = observed_phases
        except (ValueError, KeyError, TypeError, OSError) as error:
            errors.append(f"{target}/server: {error}")
    for previous,current in zip(measured_intervals,measured_intervals[1:]):
        if previous[2] > current[1]:
            errors.append(f"measured phases overlap/out of order: {previous[0]} and {current[0]}")
    status = "invalid" if errors else "validated-consumer-evidence"
    write_status = dict(schema_version=1, status=status, errors=errors, scope="Client receipts, original live owner calls, typed random preservation and separate deterministic fixture only; not full matrix or crash qualification")
    if phase_stats:
        write_status["phase_stats"] = dict(schema_version=1, observations=phase_counter_rows,
            scope="Read-only non-atomic original-owner cumulative counter differences across client phases; concurrent maintenance may overlap. Summed durations are not fractions of wall time.")
    (out / "validation.json").write_text(json.dumps(write_status, indent=2, allow_nan=False)+"\n")
    headers = ["target", "phase", "repeat", "operation", "count", "ops_per_sec", "avg_us", "p50_us", "p95_us", "p99_us", "p999_us", "max_us", "operation_error_count", "valid"]
    for row in rows:
        row["valid"] = "false" if errors else "true"
    with (out / "summary.tsv").open("w") as stream:
        writer = csv.DictWriter(stream, fieldnames=headers, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    (out / "summary.md").write_text("# YCSB comparison\n\nStatus: " + status + "\n\nTOTAL excludes original client close; close timings remain in receipts.\n\n" + "\n".join("- " + error for error in errors) + "\n")
    for error in errors:
        print(error, file=sys.stderr)
    return 3 if errors else 0


EXE_HASH_CACHE = {}


def exe_digest(path):
    # Hash the executable before launch; /proc/exe must resolve to that same
    # actual inode and metadata. Avoid delaying observation by hashing a large
    # ELF after a short-lived client has already completed.
    stat = os.stat(path)
    key = (stat.st_dev,stat.st_ino,stat.st_size,stat.st_mtime_ns,stat.st_ctime_ns)
    if key not in EXE_HASH_CACHE:
        value = digest(path)
        after = os.stat(path)
        # Reading the executable may itself update atime; it is not content or
        # identity. Keep every stable field in the cache key guarded instead.
        after_key = (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns)
        require(after_key == key, "executable changed while hashing")
        EXE_HASH_CACHE[key] = value
    return EXE_HASH_CACHE[key]


def observe_core(pid):
    proc = pathlib.Path(f"/proc/{pid}")
    stat = (proc / "stat").read_text().rsplit(")", 1)[1].split()
    return dict(pid=pid, start_ticks=int(stat[19]), exe_sha256=exe_digest(proc / "exe"))


def observe(pid):
    identity = observe_core(pid)
    raw_environment = pathlib.Path(f"/proc/{pid}/environ").read_bytes()
    require(same(observe_core(pid),{key:identity[key] for key in ("pid","start_ticks","exe_sha256")}), "process changed during observation")
    require(bool(raw_environment) and raw_environment.endswith(b"\0"), "empty or unterminated child environment")
    environment = raw_environment[:-1].split(b"\0")
    require(all(entry and b"=" in entry and entry.partition(b"=")[0] for entry in environment), "malformed child environment")
    require(not any(item.partition(b"=")[0] == b"GOMAXPROCS" for item in environment), "child GOMAXPROCS was not unset")
    identity["gomaxprocs_unset"] = True
    return identity


def validate_cpu_policy(policy, identity):
    require(isinstance(policy,dict) and same(policy.get("process"),identity), "missing or mismatched process CPU policy")
    affinity=policy.get("leader_affinity")
    require(isinstance(affinity,list) and len(affinity)>0 and all(type(x) is int and x>=0 for x in affinity) and affinity==sorted(set(affinity)), "invalid CPU affinity")
    cgroup=policy.get("cgroup")
    require(isinstance(cgroup,dict) and isinstance(cgroup.get("membership"),str) and isinstance(cgroup.get("ancestors"),list) and len(cgroup["ancestors"])>0, "missing ancestor CPU quota evidence")


def validate_server_policies(directory):
    for prefix in ("server", "reopened", "fixture-write", "fixture-reopened"):
        observed=read(directory / f"{prefix}.launch.json")
        validate_cpu_policy(observed.get("cpu_policy"),observed["process"])


def observe_cpu_policy(pid, identity):
    require(same(observe(pid),identity),"process changed before CPU policy observation")
    affinity=sorted(os.sched_getaffinity(pid))
    cgroup=runtime_observer.quota_snapshot(pid)
    require(affinity==sorted(os.sched_getaffinity(pid)),"leader affinity changed during CPU policy observation")
    require(same(observe(pid),identity),"process changed during CPU policy observation")
    policy=dict(process=identity,leader_affinity=affinity,cgroup=cgroup,scope="leader affinity and exported ancestor CPU constraints; sampled only")
    validate_cpu_policy(policy,identity)
    return policy


def validate_client_runtime(value,identity):
    require(isinstance(value,dict) and value.get("status")=="actual-child-runtime-observed" and type(value.get("sample_count")) is int and value["sample_count"]>0, "missing actual client runtime observation")
    require(value.get("observer_source_sha256")==RUNTIME_HELPER_SHA, "unbound runtime observer source")
    require(value.get("whole_interval_verified") is False and value.get("unperturbed_timing_verified") is False, "invalid runtime sampling scope")
    samples=value.get("samples")
    require(isinstance(samples,list) and len(samples)==value["sample_count"] and same(samples[0],value.get("first")) and same(samples[-1],value.get("last")), "incomplete runtime samples")
    for sample in samples:
        require(isinstance(sample,dict) and sample.get("pid")==identity["pid"] and sample.get("start_ticks")==identity["start_ticks"] and sample.get("GOMAXPROCS_present") is False, "runtime process identity mismatch")
        require(type(sample.get("runtime_gomaxprocs")) is int and sample["runtime_gomaxprocs"]>0 and sample.get("address")==hex(OBSERVED_CLIENT_SYMBOLS.get(identity.get("exe_sha256"),0)), "invalid actual client runtime")
        validate_cpu_policy(dict(process=identity,leader_affinity=sample.get("leader_affinity"),cgroup=sample.get("cgroup")),identity)
        require(all(same(sample.get(key),samples[0].get(key)) for key in ("runtime_gomaxprocs","leader_affinity","cgroup")),"runtime or CPU policy changed across samples")


def observe_owned(pid, data_dir, output, expected=None):
    # Preserve safe cleanup identity before reading optional qualification data.
    pinned = observe_core(pid)
    write_new(output,dict(process=pinned,data_dir=data_dir))
    deadline=time.monotonic()+2
    while True:
        actual=observe_core(pid)
        require(actual["pid"] == pinned["pid"] and actual["start_ticks"] == pinned["start_ticks"], "owned launch start identity changed")
        pathlib.Path(output).write_text(json.dumps(dict(process=actual,data_dir=data_dir),allow_nan=False)+"\n")
        if expected is None or actual["exe_sha256"] == expected:
            identity=observe(pid)
            policy=observe_cpu_policy(pid,identity)
            pathlib.Path(output).write_text(json.dumps(dict(process=identity,data_dir=data_dir,cpu_policy=policy),allow_nan=False)+"\n")
            return
        require(time.monotonic()<deadline,"owned executable never matched requested ELF; refusing clients")
        time.sleep(.01)



def terminate_owned(identity, timeout=10):
    """Pin the process before checking identity; never signal by a reusable PID."""
    require(type(identity.get("pid")) is int and identity["pid"] > 0 and type(identity.get("start_ticks")) is int and identity["start_ticks"] > 0 and SHA.fullmatch(identity.get("exe_sha256","")), "missing cleanup process identity")
    require(hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal"), "safe pidfd cleanup unavailable")
    try:
        descriptor = os.pidfd_open(identity["pid"])
    except ProcessLookupError:
        return  # Already gone; the parent still checks its retained wait status.
    try:
        def exited():
            return bool(select.select([descriptor], [], [], 0)[0])
        def checked_signal(signum):
            if exited():
                return
            actual = observe_core(identity["pid"])
            require(same(actual,{key:identity[key] for key in ("pid","start_ticks","exe_sha256")}), "cleanup process identity mismatch; refusing signal")
            signal.pidfd_send_signal(descriptor, signum)
        checked_signal(signal.SIGTERM)
        if not select.select([descriptor], [], [], timeout)[0]:
            checked_signal(signal.SIGKILL)
            require(select.select([descriptor], [], [], timeout)[0], "owned process did not terminate")
    finally:
        os.close(descriptor)

def terminate_direct_child(pid, start_ticks, parent_pid, output, timeout=10):
    """Fallback only for the shell's unreaped child before a launch receipt exists."""
    require(type(pid) is int and pid > 0 and type(start_ticks) is int and start_ticks > 0 and type(parent_pid) is int and parent_pid > 0, "missing direct-child identity; refusing signal")
    identity=observe_core(pid)
    stat=pathlib.Path(f"/proc/{pid}/stat").read_text().rsplit(")",1)[1].split()
    require(identity["start_ticks"]==start_ticks and int(stat[19])==start_ticks and int(stat[1])==parent_pid, "direct-child identity mismatch; refusing signal")
    # terminate_owned pins a pidfd and rechecks start/executable before each signal.
    write_new(output,dict(schema_version=1,kind="pre_observation_owned_child_cleanup",process=identity,parent_pid=parent_pid,termination_complete=False))
    terminate_owned(identity,timeout)
    pathlib.Path(output).write_text(json.dumps(dict(schema_version=1,kind="pre_observation_owned_child_cleanup",process=identity,parent_pid=parent_pid,termination_complete=True),allow_nan=False)+"\n")


def positive_timeout(value, label):
    require(type(value) in (int,float) and math.isfinite(value) and value > 0, f"{label} must be finite and positive")
    return value


def same_timeout(actual,expected):
    # Seconds are configurable finite durations, not identity/count fields.
    # argparse emits 600.0 for shell input 600; both denote the same deadline.
    positive_timeout(actual,"recorded deadline")
    positive_timeout(expected,"manifest deadline")
    return actual == expected


@contextlib.contextmanager
def outer_deadline(seconds):
    """Interrupt Python work once; nested child cleanup keeps its own budget."""
    positive_timeout(seconds,"outer deadline")
    previous_handler=signal.getsignal(signal.SIGALRM)
    previous_timer=signal.getitimer(signal.ITIMER_REAL)
    require(previous_timer == (0.0,0.0), "refusing to replace an active outer deadline")
    def expired(signum,frame):
        raise TimeoutError("outer phase/helper wall deadline exceeded")
    expired.ycsb_outer_deadline=True
    expired.ycsb_outer_seconds=seconds
    signal.signal(signal.SIGALRM,expired)
    signal.setitimer(signal.ITIMER_REAL,seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL,0)
        signal.signal(signal.SIGALRM,previous_handler)


def terminate_pinned_child(descriptor, timeout):
    """Fallback only for a direct Popen child pinned before any wait/poll.

    The descriptor, rather than a reusable PID, authorizes signals to the exact
    direct child even if it execs again or procfs observation fails. External
    server cleanup still uses terminate_owned and refuses identity mismatches.
    """
    def exited(): return bool(select.select([descriptor],[],[],0)[0])
    def send(signum):
        try: signal.pidfd_send_signal(descriptor,signum)
        except ProcessLookupError:
            require(exited(),"pinned process disappeared without observed exit")
    if not exited(): send(signal.SIGTERM)
    if not select.select([descriptor],[],[],timeout)[0]:
        send(signal.SIGKILL)
        require(select.select([descriptor],[],[],timeout)[0],"pinned owned child did not terminate")


CANCELLATION_SIGNALS = {signal.SIGINT,signal.SIGTERM,signal.SIGALRM}


@contextlib.contextmanager
def defer_startup_cancellation():
    # Queue rather than block signals while spawning: the executed child must
    # inherit its normal signal mask. No cancellation can interrupt Popen's
    # return/assignment or the pin acquisition before ownership is recorded.
    pending=[]
    handlers={signum:signal.getsignal(signum) for signum in CANCELLATION_SIGNALS}
    def remember(signum,frame): pending.append(signum)
    try:
        for signum in CANCELLATION_SIGNALS: signal.signal(signum,remember)
        yield pending
    finally:
        for signum,handler in handlers.items(): signal.signal(signum,handler)


def terminate_unpinned_child(child,timeout):
    """Pin failure fallback: a direct, unreaped child retains its PID.

    waitid(WNOWAIT) verifies parent ownership before any numeric-PID signal and
    never releases that identity. Only this launcher's Popen object is accepted;
    external servers continue to require observed identity/pidfd validation.
    """
    for signum in (signal.SIGTERM,signal.SIGKILL):
        require(child.returncode is None,"refusing signal after direct child was reaped")
        status=os.waitid(os.P_PID,child.pid,os.WEXITED|os.WNOHANG|os.WNOWAIT)
        if status is not None: return child.wait(timeout=timeout)
        try: os.kill(child.pid,signum)
        except ProcessLookupError: pass  # Retain ownership until checked wait.
        try: return child.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            if signum == signal.SIGKILL: raise


def runtime_client_preflight(client):
    """Execution support is limited to independently inspected Linux ELF pins."""
    require(sys.platform == "linux" and platform.machine().lower() in ("x86_64","amd64") and hasattr(os,"pidfd_open") and pathlib.Path('/proc/self/stat').is_file(),"client runtime observation requires Linux x86_64, pidfd and /proc")
    require(client.get("exe_sha256") in OBSERVED_CLIENT_SYMBOLS,"required client runtime has no exact ELF/symbol binding")


def resolve_launch_role(manifest, command, executable_hash, role):
    require(role in ("client","fixture","control","docker"), "unknown executable role")
    if manifest.get("require_client_runtime_observation") is True:
        runtime_client_preflight(manifest["client"])
    if manifest["client"]["exe_sha256"] not in OBSERVED_CLIENT_SYMBOLS:
        require(manifest.get("require_client_runtime_observation") is not True,"required client runtime has no exact ELF/symbol binding")
        return False
    if role == "client":
        require(executable_hash == manifest["client"]["exe_sha256"],"controlled client executable mismatch")
        return True
    require(executable_hash not in OBSERVED_CLIENT_SYMBOLS,"controlled client cannot use helper role")
    if role in ("fixture","control"):
        binding=manifest[role]
        require(command[0] == binding["binary"] and executable_hash == binding["exe_sha256"],role+" executable mismatch")
    else:
        require(command[0] == manifest["docker_binary"] and executable_hash == manifest["server_builds"]["mongodb"]["docker_cli_sha256"],"docker executable mismatch")
    return False


def launch(args):
    out, stem = pathlib.Path(args.directory), args.stem
    receipt = out / f"{stem}.close.json"
    require(not receipt.exists(), "refusing reused close receipt")
    timeout=positive_timeout(getattr(args,"timeout",600),"child timeout")
    cleanup_timeout=positive_timeout(getattr(args,"cleanup_timeout",10),"child cleanup timeout")
    outer_timeout=getattr(signal.getsignal(signal.SIGALRM),"ycsb_outer_seconds",None)
    manifest = read(out.parent / "manifest.json")
    phase_id = f'{manifest["campaign_id"]}:{out.name}:{stem}:{uuid.uuid4().hex}'
    env = os.environ.copy()
    env.pop("GOMAXPROCS", None)
    env.update(YCSB_CLOSE_RECEIPT_PATH=str(receipt), YCSB_PHASE_ID=phase_id, YCSB_CLIENT_BUILD_ID=manifest["client"]["build_id"], YCSB_PROFILE=manifest["profile"])
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    executable_hash=exe_digest(command[0])
    role = getattr(args,"role","client")
    observe_runtime = resolve_launch_role(manifest,command,executable_hash,role)
    observer = runtime_observer.RuntimeObserver(command[0],executable_hash,OBSERVED_CLIENT_SYMBOLS[executable_hash]) if observe_runtime else None
    import ycsb_compare_live
    phase_stats = ycsb_compare_live.phase_stats_enabled(manifest) and role == "client" and (
        stem == "load" or re.fullmatch(r"run_[1-9][0-9]*", stem) is not None)
    phase_boundaries = {}
    if phase_stats:
        phase_boundaries["before"] = ycsb_compare_live.phase_stats_rpc(out, stem, "before", phase_id, manifest)
    started = now()
    deadline=time.monotonic()+timeout
    identity=None
    timed_out=False
    child=None
    descriptor=None
    runtime_evidence=None
    startup_signals=[]
    previous_handler=signal.getsignal(signal.SIGTERM)
    def interrupted(signum, frame):
        raise InterruptedError(f"launcher interrupted by signal {signum}")
    with (out / f"{stem}.out").open("x") as stdout, (out / f"{stem}.err").open("x") as stderr:
        try:
            signal.signal(signal.SIGTERM, interrupted)
            with defer_startup_cancellation() as startup_signals:
                child = subprocess.Popen(command, env=env, stdout=stdout, stderr=stderr, start_new_session=True)
                # No wait/poll can release this direct child's PID before pinning.
                descriptor=os.pidfd_open(child.pid)
            if startup_signals:
                if signal.SIGALRM in startup_signals:
                    raise TimeoutError("outer phase/helper wall deadline exceeded during startup")
                raise InterruptedError(f"launcher interrupted during startup by signals {startup_signals}")
            write_new(out / f"{stem}.cleanup.json",dict(pid=child.pid,pidfd_pinned=True))
            identity = observe_core(child.pid)
            (out / f"{stem}.cleanup.json").write_text(json.dumps(dict(process=identity,pidfd_pinned=True),allow_nan=False)+"\n")
            identity = observe(child.pid)
            if observer is None:
                code = child.wait(timeout=max(0,deadline-time.monotonic()))
            else:
                class UnreapedChild:
                    pid=child.pid
                    def poll(self):
                        return 0 if os.waitid(os.P_PID,self.pid,os.WEXITED|os.WNOHANG|os.WNOWAIT) is not None else None
                view=UnreapedChild()
                while True:
                    if select.select([descriptor],[],[],0)[0]: break
                    runtime_observer.sample_owned_or_exited(observer,view,lambda wait: bool(select.select([descriptor],[],[],wait)[0]),min(runtime_observer.EXIT_RECHECK_SECONDS,max(0,deadline-time.monotonic())))
                    remaining=deadline-time.monotonic()
                    if remaining<=0: raise subprocess.TimeoutExpired(command,timeout)
                    interval=runtime_observer.sample_interval(observer,RUNTIME_SAMPLE_SECONDS)
                    if select.select([descriptor],[],[],min(interval,remaining))[0]: break
                runtime_evidence=observer.validate()
                runtime_evidence.update(samples=observer.samples,sample_interval_seconds=RUNTIME_SAMPLE_SECONDS,startup_sample_interval_seconds=runtime_observer.STARTUP_POLL_SECONDS,observer_source_sha256=RUNTIME_HELPER_SHA,observer_scope="read-only live samples may overlap and perturb timed work; not a continuous measurement; startup cadence ends at first complete sample")
                validate_client_runtime(runtime_evidence,identity)
                code=child.wait(timeout=max(0,deadline-time.monotonic()))
            if time.monotonic() > deadline:
                raise subprocess.TimeoutExpired(command,timeout)
        except BaseException as error:
            # Defer every cancellation throughout TERM/KILL/reap AND failure
            # receipt retention. Pending cancellation is delivered only once
            # the child is reaped and that receipt has been written.
            previous_mask=signal.pthread_sigmask(signal.SIG_BLOCK,CANCELLATION_SIGNALS)
            try:
                if getattr(signal.getsignal(signal.SIGALRM),"ycsb_outer_deadline",False):
                    signal.setitimer(signal.ITIMER_REAL,0)
                timed_out=isinstance(error,(subprocess.TimeoutExpired,TimeoutError)) or signal.SIGALRM in startup_signals
                cleanup=dict(attempted=child is not None,success=False,reaped=False,error="",exit_code=None,method="popen-child-pidfd-before-any-wait" if descriptor is not None else "popen-direct-child-unreaped")
                try:
                    if child is None:
                        cleanup.update(success=True,method="no-child-created")
                    elif descriptor is None:
                        code=terminate_unpinned_child(child,cleanup_timeout)
                        cleanup.update(success=True,reaped=True,exit_code=code)
                    else:
                        terminate_pinned_child(descriptor,cleanup_timeout)
                        code=child.wait(timeout=cleanup_timeout)
                        cleanup.update(success=True,reaped=True,exit_code=code)
                except BaseException as cleanup_error:
                    cleanup["error"]=str(cleanup_error)
                cleanup["pending_cancellation_signals"]=sorted(signal.sigpending() & CANCELLATION_SIGNALS)
                write_new(out/f"{stem}.failure.json",dict(schema_version=1,kind="owned_child_launcher_failure",campaign_id=manifest["campaign_id"],phase_id=phase_id,process=identity,pid=child.pid if child is not None else None,pidfd_pinned=descriptor is not None,startup_cancellation_signals=startup_signals,start_utc=started,end_utc=now(),timeout_seconds=timeout,cleanup_timeout_seconds=cleanup_timeout,outer_timeout_seconds=outer_timeout,timed_out=timed_out,error="child wait deadline exceeded" if isinstance(error,subprocess.TimeoutExpired) else str(error),runtime_diagnostics=observer.diagnostics() if observer is not None else None,runtime_samples=observer.samples if observer is not None else None,cleanup=cleanup))
                require(cleanup["success"],"owned child cleanup failed: "+cleanup["error"])
                if not isinstance(error,subprocess.TimeoutExpired):
                    raise
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK,previous_mask)
        finally:
            signal.signal(signal.SIGTERM,previous_handler)
            if descriptor is not None: os.close(descriptor)
    # Capture the unchanged child interval before observing its owner again.
    ended = now()
    record = dict(schema_version=1, campaign_id=manifest["campaign_id"], phase_id=phase_id, process=identity, runtime_observation=runtime_evidence, start_utc=started, end_utc=ended, exit_code=code, timeout_seconds=timeout,cleanup_timeout_seconds=cleanup_timeout,outer_timeout_seconds=outer_timeout,timed_out=timed_out, output_sha256=digest(out / f"{stem}.out"), receipt_sha256=digest(receipt) if receipt.exists() else None)
    if phase_stats:
        record["phase_stats"] = phase_boundaries
        try:
            phase_boundaries["after"] = ycsb_compare_live.phase_stats_rpc(out, stem, "after", phase_id, manifest)
        finally:
            # A failed after observation cannot discard the actual reaped
            # child's original outcome or interval, even when it exited zero.
            write_new(out / f"{stem}.launch.json", record)
    else:
        write_new(out / f"{stem}.launch.json", record)
    return 124 if timed_out else code if code > 0 else 3 if code < 0 else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    check = sub.add_parser("validate")
    check.add_argument("out", type=pathlib.Path)
    run = sub.add_parser("launch")
    run.add_argument("--timeout",type=float,default=600)
    run.add_argument("--cleanup-timeout",type=float,default=10)
    run.add_argument("--outer-timeout",type=float)
    run.add_argument("directory")
    run.add_argument("stem")
    run.add_argument("command", nargs=argparse.REMAINDER)
    proc = sub.add_parser("observe")
    proc.add_argument("pid", type=int)
    proc.add_argument("data_dir")
    proc.add_argument("output")
    proc.add_argument("--await-exec")
    stop = sub.add_parser("stop")
    stop.add_argument("launch", type=pathlib.Path)
    stop.add_argument("--timeout", type=float, default=10)
    direct = sub.add_parser("stop-child")
    direct.add_argument("pid",type=int)
    direct.add_argument("start_ticks",type=int)
    direct.add_argument("parent_pid",type=int)
    direct.add_argument("output",type=pathlib.Path)
    direct.add_argument("--timeout",type=float,default=10)
    args = parser.parse_args()
    try:
        if args.mode == "validate":
            # Invalidate an earlier success even if manifest decoding now fails.
            for summary in ("summary.tsv", "summary.md"):
                (args.out / summary).unlink(missing_ok=True)
            (args.out / "validation.json").write_text('{"status":"invalid","errors":["validation incomplete"]}\n')
            return validate(args.out)
        if args.mode == "launch":
            with outer_deadline(args.outer_timeout if args.outer_timeout is not None else args.timeout+3*args.cleanup_timeout+10):
                return launch(args)
        if args.mode == "stop-child":
            terminate_direct_child(args.pid,args.start_ticks,args.parent_pid,args.output,positive_timeout(args.timeout,"child cleanup timeout"))
            return 0
        if args.mode == "stop":
            terminate_owned(read(args.launch)["process"], args.timeout)
            return 0
        observe_owned(args.pid,args.data_dir,args.output,digest(args.await_exec) if args.await_exec else None)
        return 0
    except (ValueError, KeyError, TypeError, OSError) as error:
        if isinstance(error,TimeoutError) and args.mode == "launch":
            failure=pathlib.Path(args.directory)/(args.stem+".failure.json")
            if not failure.exists():
                write_new(failure,dict(schema_version=1,kind="phase_outer_deadline_failure",error=str(error),timed_out=True,end_utc=now()))
        print(f"invalid YCSB evidence: {error}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    sys.exit(main())
