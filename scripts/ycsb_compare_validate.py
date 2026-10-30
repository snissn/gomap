#!/usr/bin/env python3
"""Fail-closed consumer for go-ycsb close receipt v1 (snissn/go-ycsb#7).

runtime.json and lifecycle.json are independent, externally produced observations;
this tool never manufactures server COW, checkpoint or reopen evidence. See
scripts/ycsb_compare_receipts.md for the observation contract.
"""
import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import pathlib
import re
import signal
import select
import subprocess
import sys
import time
import uuid

PRODUCER = "3c8148495b8d4490cf0ec30e9eb92d14a7a71bac"
TARGETS = ("treedb_native", "treedb_mongo", "mongodb")
NUM = r"[0-9]+(?:\.[0-9]+)?"
ROW = re.compile(r"^([A-Z_]+)\s+- Takes\(s\): (" + NUM + r"), Count: ([0-9]+), OPS: (" + NUM + r"), Avg\(us\): (" + NUM + r"), Min\(us\): (" + NUM + r"), Max\(us\): (" + NUM + r"), 50th\(us\): (" + NUM + r"), 90th\(us\): (" + NUM + r"), 95th\(us\): (" + NUM + r"), 99th\(us\): (" + NUM + r"), 99.9th\(us\): (" + NUM + r"), 99.99th\(us\): (" + NUM + r")$")
SHA = re.compile(r"[0-9a-f]{64}$")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    with open(path, "rb") as stream:
        value = hashlib.sha256()
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
        return value.hexdigest()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f"duplicate JSON key {key}")
        result[key] = value
    return result


def read(path):
    with open(path, encoding="utf-8") as stream:
        return json.load(stream, object_pairs_hook=unique_object)


def write_new(path, value):
    with open(path, "x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")


def timestamp(value):
    require(isinstance(value, str) and value.endswith("Z") and not value.startswith("0001"), "invalid UTC timestamp")
    value = re.sub(r"(\.\d{6})\d+(?=Z$)", r"\1", value)
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def result(value, label, timed=True, bounds=None):
    require(isinstance(value, dict), f"missing {label}")
    require(value.get("attempted") is True and value.get("success") is True and value.get("error") == "", f"failed/skipped {label}")
    if timed:
        start, end = timestamp(value["start_utc"]), timestamp(value["end_utc"])
        elapsed = value["elapsed_ns"]
        require(type(elapsed) is int and elapsed >= 0 and start <= end, f"invalid {label} duration")
        require(abs((end-start)*1e9-elapsed) <= 2_000_000, f"inconsistent {label} duration")
        if bounds:
            require(bounds[0] <= start <= end <= bounds[1], f"stale/out-of-order {label}")
        return start, end


def process(value):
    require(type(value.get("pid")) is int and value["pid"] > 0, "missing child PID")
    require(type(value.get("start_ticks")) is int and value["start_ticks"] > 0, "missing child start identity")
    require(SHA.fullmatch(value.get("exe_sha256", "")), "missing observed ELF hash")
    require(value.get("gomaxprocs_unset") is True, "child GOMAXPROCS was not unset")


def parse_log(path):
    rows = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        match = ROW.fullmatch(line)
        if not match:
            require(not re.match(r"^[A-Z_]+\s+- Takes", line), f"malformed measurement row in {path.name}")
            continue
        op, takes, count, ops, avg, minimum, maximum, p50, p90, p95, p99, p999, p9999 = match.groups()
        # Duplicate/intermediate blocks need explicit disposition, never last-row wins.
        require(op not in rows, f"duplicate {op} row in {path.name}")
        rows[op] = dict(operation=op, count=int(count), ops_per_sec=float(ops), avg_us=float(avg), p50_us=float(p50), p95_us=float(p95), p99_us=float(p99), p999_us=float(p999), max_us=float(maximum))
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
    launch = read(directory / f"{stem}.launch.json")
    receipt_path = directory / f"{stem}.close.json"
    receipt = read(receipt_path)
    process(launch["process"])
    client = manifest["client"]
    require(launch["process"]["exe_sha256"] == client["exe_sha256"], "client ELF mismatch")
    require(launch["exit_code"] == 0, "client exit failure")
    require(launch["output_sha256"] == digest(directory / f"{stem}.out"), "stdout hash mismatch")
    require(launch["receipt_sha256"] == digest(receipt_path), "receipt hash mismatch")
    require(launch["campaign_id"] == manifest["campaign_id"], "stale launcher campaign")
    require(receipt["schema_version"] == 1 and receipt["total_excludes_close"] is True, "unsupported close receipt")
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
        require(native["applicable"] is True and native["flush_enabled"] is True and type(native["ack"]) is int and native["ack"] == 1, "required native flush/visible ACK missing")
        dial = result(native["dial"], "native dial", bounds=db_bounds)
        flush = result(native["flush"], "native FlushAll", bounds=(dial[1], db_bounds[1]))
        result(native["connection_close"], "native connection Close", bounds=(flush[1], db_bounds[1]))
    else:
        require(native["applicable"] is False and native["flush_enabled"] is False, "unexpected native flush")
    return launch["phase_id"]


def server_evidence(directory, target, manifest, phase_ids):
    runtime = read(directory / "runtime.json")
    lifecycle = read(directory / "lifecycle.json")
    launch = read(directory / "server.launch.json")
    process(launch["process"])
    build = manifest["server_builds"][target]
    server_manifest = read(directory.parent / "server-build.json")
    require(server_manifest["schema_version"] == 1 and server_manifest["servers"][target] == build, "server build manifest mismatch")
    require(build["schema_version"] == 1 and isinstance(build["version"], str) and build["version"], "missing server version binding")
    require(re.fullmatch(r"[0-9a-f]{40}", build["source_commit"]), "missing server source binding")
    require(SHA.fullmatch(build["exe_sha256"]) and launch["process"]["exe_sha256"] == build["exe_sha256"], "server ELF binding mismatch")
    require(build["expected_owner"] in ({"mongodb"} if target == "mongodb" else {"legacy", "cow"}), "invalid expected owner")
    for evidence in (runtime, lifecycle):
        require(evidence["version"] == build["version"] and evidence["source_commit"] == build["source_commit"], "server version/source mismatch")
        require(evidence["schema_version"] == 1 and evidence["campaign_id"] == manifest["campaign_id"], "stale server evidence")
        require(evidence["target"] == target and evidence["profile"] == manifest["profile"] and evidence["format"] == manifest["formats"][target], "server settings mismatch")
        require(evidence["process"] == launch["process"], "server process identity mismatch")
        require(evidence["data_dir"] == launch["data_dir"] and pathlib.Path(evidence["data_dir"]).is_absolute(), "data directory mismatch")
        require(evidence["owner_id"] and evidence["owner_id"] == runtime["owner_id"], "owner mismatch")
    require(runtime["phase_ids"] == phase_ids, "runtime phase identity mismatch")
    require(type(runtime["observed_gomaxprocs"]) is int and runtime["observed_gomaxprocs"] > 0, "missing actual runtime GOMAXPROCS")
    require(runtime["backend"] == build["expected_owner"], "actual owner/backend does not match selected version")
    require(isinstance(runtime["observation"], str) and runtime["observation"], "missing runtime witness provenance")
    require(lifecycle["phase_ids"] == phase_ids, "lifecycle phase identity mismatch")
    require(lifecycle["server_exit_code"] == 0, "server close exit failure")
    settled = max(timestamp(read(path)["end_utc"]) for path in directory.glob("*.launch.json") if path.name != "server.launch.json")
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
    require(stopped["process"] == launch["process"] and stopped["exit_code"] == 0, "owned service cleanup failed")


def validate(out):
    manifest = read(out / "manifest.json")
    require(manifest["schema_version"] == 1 and manifest["campaign_id"], "missing manifest identity")
    require(manifest["profile"] in {"command_wal_durable", "command_wal_relaxed", "no_wal_fast"}, "unsupported profile")
    for key in ("recordcount", "operationcount", "threadcount", "run_repeats"):
        require(type(manifest[key]) is int and manifest[key] > 0, f"invalid {key}")
    client = manifest["client"]
    require(client["producer_commit"] == PRODUCER and re.fullmatch(r"[0-9a-f]{40}", client["source_commit"]), "unbound producer source")
    require(client["build_id"] and client["toolchain"] and client["compatibility_configuration"], "incomplete client build binding")
    require(client == read(out / "client-build.json"), "client build manifest mismatch")
    require(client["source_patch_sha256"] == digest(out / "client.patch"), "client patch mismatch")
    require((out / "client-buildinfo.txt").read_text().strip(), "missing actual client build info")
    require(SHA.fullmatch(manifest["client"]["exe_sha256"]), "missing client build hash")
    require(manifest["targets"] and len(set(manifest["targets"])) == len(manifest["targets"]) and set(manifest["targets"]) <= set(TARGETS), "invalid targets")
    rows, errors, seen_ids = [], [], set()
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
            except (ValueError, KeyError, TypeError, OSError) as error:
                errors.append(f"{target}/{stem}: {error}")
        try:
            server_evidence(directory, target, manifest, ids)
        except (ValueError, KeyError, TypeError, OSError) as error:
            errors.append(f"{target}/server: {error}")
    status = "invalid" if errors else "validated-consumer-evidence"
    write_status = dict(schema_version=1, status=status, errors=errors, scope="Client logs/receipts and supplied server observations only; not full matrix or scientific qualification")
    (out / "validation.json").write_text(json.dumps(write_status, indent=2)+"\n")
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


def observe_core(pid):
    proc = pathlib.Path(f"/proc/{pid}")
    stat = (proc / "stat").read_text().rsplit(")", 1)[1].split()
    return dict(pid=pid, start_ticks=int(stat[19]), exe_sha256=digest(proc / "exe"))


def observe(pid):
    identity = observe_core(pid)
    environment = pathlib.Path(f"/proc/{pid}/environ").read_bytes().split(b"\0")
    identity["gomaxprocs_unset"] = not any(item.startswith(b"GOMAXPROCS=") for item in environment)
    require(observe_core(pid) == {key:identity[key] for key in ("pid","start_ticks","exe_sha256")}, "process changed during observation")
    return identity


def observe_owned(pid, data_dir, output, expected=None):
    # Preserve safe cleanup identity before reading optional qualification data.
    pinned = observe_core(pid)
    write_new(output,dict(process=pinned,data_dir=data_dir))
    deadline=time.monotonic()+2
    while True:
        actual=observe_core(pid)
        require(actual["pid"] == pinned["pid"] and actual["start_ticks"] == pinned["start_ticks"], "owned launch start identity changed")
        pathlib.Path(output).write_text(json.dumps(dict(process=actual,data_dir=data_dir))+"\n")
        if expected is None or actual["exe_sha256"] == expected:
            identity=observe(pid)
            pathlib.Path(output).write_text(json.dumps(dict(process=identity,data_dir=data_dir))+"\n")
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
            require(actual == {key:identity[key] for key in ("pid","start_ticks","exe_sha256")}, "cleanup process identity mismatch; refusing signal")
            signal.pidfd_send_signal(descriptor, signum)
        checked_signal(signal.SIGTERM)
        if not select.select([descriptor], [], [], timeout)[0]:
            checked_signal(signal.SIGKILL)
            require(select.select([descriptor], [], [], timeout)[0], "owned process did not terminate")
    finally:
        os.close(descriptor)

def launch(args):
    out, stem = pathlib.Path(args.directory), args.stem
    receipt = out / f"{stem}.close.json"
    require(not receipt.exists(), "refusing reused close receipt")
    manifest = read(out.parent / "manifest.json")
    phase_id = f'{manifest["campaign_id"]}:{out.name}:{stem}:{uuid.uuid4().hex}'
    env = os.environ.copy()
    env.pop("GOMAXPROCS", None)
    env.update(YCSB_CLOSE_RECEIPT_PATH=str(receipt), YCSB_PHASE_ID=phase_id, YCSB_CLIENT_BUILD_ID=manifest["client"]["build_id"], YCSB_PROFILE=manifest["profile"])
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    started = now()
    with (out / f"{stem}.out").open("x") as stdout, (out / f"{stem}.err").open("x") as stderr:
        child = subprocess.Popen(command, env=env, stdout=stdout, stderr=stderr, start_new_session=True)
        def interrupted(signum, frame):
            raise InterruptedError(f"launcher interrupted by signal {signum}")
        signal.signal(signal.SIGTERM, interrupted)
        try:
            # Popen has completed exec; this reads the actual executable and environment.
            identity = observe(child.pid)
            code = child.wait()
        except BaseException:
            # Cleanup only the observed child through a pinned process descriptor.
            # If initial observation failed, no unknown process is signaled.
            if "identity" in locals():
                terminate_owned(identity)
                child.wait()
            raise
    write_new(out / f"{stem}.launch.json", dict(schema_version=1, campaign_id=manifest["campaign_id"], phase_id=phase_id, process=identity, start_utc=started, end_utc=now(), exit_code=code, output_sha256=digest(out / f"{stem}.out"), receipt_sha256=digest(receipt) if receipt.exists() else None))
    return code if code > 0 else 3 if code < 0 else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    check = sub.add_parser("validate")
    check.add_argument("out", type=pathlib.Path)
    run = sub.add_parser("launch")
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
    args = parser.parse_args()
    try:
        if args.mode == "validate":
            # Invalidate an earlier success even if manifest decoding now fails.
            for summary in ("summary.tsv", "summary.md"):
                (args.out / summary).unlink(missing_ok=True)
            (args.out / "validation.json").write_text('{"status":"invalid","errors":["validation incomplete"]}\n')
            return validate(args.out)
        if args.mode == "launch":
            return launch(args)
        if args.mode == "stop":
            terminate_owned(read(args.launch)["process"], args.timeout)
            return 0
        observe_owned(args.pid,args.data_dir,args.output,digest(args.await_exec) if args.await_exec else None)
        return 0
    except (ValueError, KeyError, TypeError, OSError) as error:
        print(f"invalid YCSB evidence: {error}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    sys.exit(main())
