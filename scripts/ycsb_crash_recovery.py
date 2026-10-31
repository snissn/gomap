#!/usr/bin/env python3
"""Separate public ordinary-ACK process-crash control; never a performance sample.

SIGKILL is a process crash, not power loss. No explicit checkpoint is requested;
background checkpoint exclusion and actual WAL replay remain unproven.
"""
import argparse
import base64
import contextlib
import json
import os
import pathlib
import select
import signal
import socket
import subprocess
import sys
import time
import uuid

import ycsb_compare_validate as c
import ycsb_compare_live as live
import ycsb_correctness_validate as exact
from ycsb_evidence_json import loads

PROFILES = ("command_wal_durable", "command_wal_relaxed", "no_wal_fast")
SCOPE = "public-ordinary-ack-owned-process-sigkill"
LIMITATION = "unproven-background-checkpoint-exclusion-and-wal-replay"


def canonical_expected(identity):
    result = dict(identity, load=[], updates=[])
    for n in range(64):
        fields = {}
        for f in range(10):
            prefix = f"load-{n:04d}-{f:02d}".encode()
            raw = base64.b64encode(prefix+b"x"*(100-len(prefix))).decode()
            fields[f"field{f}"] = (dict(type="string", value=raw) if identity["format"] == "json"
                                    else dict(type="binary", subtype=0, base64=raw))
        result["load"].append(dict(id=f"user{n:04d}", fields=fields))
    return result


def identity(manifest):
    return dict(schema_version=1, source_id=manifest["build"]["source_commit"],
                fixture_id=manifest["campaign_id"], profile=manifest["profile"],
                target="native" if manifest["target"] == "treedb_native" else "mongo",
                format=manifest["format"])


def state(expected, snapshot, stage):
    c.require(c.schema(snapshot) and all(c.same(snapshot[k], expected[k]) for k in exact.IDENTITY), "crash snapshot identity mismatch")
    c.require(snapshot["stage"] == stage and c.same(snapshot["operations"], []), "crash snapshot stage/operations mismatch")
    return exact.rows(snapshot["rows"], expected["format"])


def validate_state(expected, before, after):
    initial = exact.rows(expected["load"], expected["format"])
    c.require(c.same(before, initial), "precrash acknowledged state mismatch")
    c.require(after.keys() <= initial.keys() and all(c.same(fields, initial[key]) for key, fields in after.items()), "crash recovery invented/torn/changed typed row")
    profile = expected["profile"]
    if profile == "command_wal_durable":
        c.require(c.same(after, initial), "durable ordinary ACK lost after process crash")
    elif profile == "command_wal_relaxed":
        wanted = {row["id"] for row in expected["load"][:len(after)]}
        c.require(set(after) == wanted, "relaxed recovery is not an acknowledged-order prefix")
    else:
        c.require(profile == "no_wal_fast", "unknown crash guarantee")
    return dict(acknowledged=len(initial), recovered=len(after), lost=len(initial)-len(after))


def artifact_barrier(directory, names, db):
    """Durably retain only external evidence. Never sync the service DB."""
    hashes = {}
    started = c.now()
    for name in names:
        path = directory/name
        c.require(not path.resolve().is_relative_to(db.resolve()), "evidence artifact is inside service DB")
        with path.open("rb") as stream:
            os.fsync(stream.fileno())
        hashes[name] = c.digest(path)
    # The fresh target directory and fresh output directory must themselves be
    # published durably, in addition to the names within the target directory.
    directories = [directory, directory.parent, directory.parent.parent]
    for path in directories:
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return dict(schema_version=1, kind="external_evidence_fsync_barrier", start_utc=started, end_utc=c.now(), artifacts_sha256=hashes, synced_directory_paths=[str(p) for p in directories])


def helper(directory, stem, manifest, owner):
    receipt = c.read(directory/(stem+".close.json"))
    launch = c.read(directory/(stem+".launch.json"))
    c.require(c.schema(launch) and c.schema(receipt), "invalid crash helper schema")
    c.process(launch["process"])
    c.exit_success(launch["exit_code"], "crash helper failed")
    c.require(launch["timed_out"] is False and not (directory/(stem+".failure.json")).exists(), "crash helper timed out/failed")
    c.require(launch["campaign_id"] == manifest["campaign_id"] and launch["process"]["exe_sha256"] == manifest["fixture"]["exe_sha256"], "crash helper source/ELF mismatch")
    for key in ("timeout_seconds", "cleanup_timeout_seconds", "outer_timeout_seconds"):
        c.require(c.same_timeout(launch[key], manifest["deadlines"][key]), "crash helper deadline mismatch")
    c.require(launch["receipt_sha256"] == c.digest(directory/(stem+".close.json")) and launch["output_sha256"] == c.digest(directory/(stem+".out")), "crash helper artifact mismatch")
    c.require(receipt["kind"] == "public_process_crash_client_close" and receipt["mode"] == stem, "wrong crash helper mode")
    c.require(type(receipt["pid"]) is int and receipt["pid"] == launch["process"]["pid"] and receipt["source_id"] == manifest["build"]["source_commit"] and receipt["fixture_id"] == manifest["campaign_id"], "crash helper identity mismatch")
    c.require(receipt["result_error"] == receipt["close_error"] == "", "original crash client result/close failed")
    c.require(receipt["checkpoint_attempted"] is False and receipt["checkpoint_succeeded"] is False and receipt["checkpoint"] is None and receipt["owner_checkpoint"] is None, "crash helper introduced explicit checkpoint")
    bounds = c.timestamp(launch["start_utc"]), c.timestamp(launch["end_utc"])
    c.require(c.timestamp(owner["observed_utc"]) <= bounds[0] <= c.timestamp(receipt["started_utc"]) <= c.timestamp(receipt["ended_utc"]) <= bounds[1], "crash helper outside observed lifetime")
    needed = {"expected.json", stem+".json"}
    if stem == "crash-write": needed.add(stem+".json.acknowledged.jsonl")
    c.require(set(receipt["artifacts_sha256"]) == needed, "missing/unexpected crash helper artifact binding")
    for name, value in receipt["artifacts_sha256"].items():
        c.require(pathlib.Path(name).name == name and value == c.digest(directory/name), "crash helper digest mismatch")
    return receipt, bounds


def listener_inode(table, socket_inodes, address):
    host, port = address.rsplit(":", 1)
    c.require(host == "127.0.0.1" and 0 < int(port) < 65536, "invalid owned loopback endpoint")
    local = socket.inet_aton(host)[::-1].hex().upper()+f":{int(port):04X}"
    matches = []
    for row in table.splitlines()[1:]:
        fields = row.split()
        if len(fields) >= 10 and fields[1] == local and fields[3] == "0A":
            inode = int(fields[9])
            if inode in socket_inodes: matches.append(inode)
    c.require(len(matches) == 1, "requested listener is not owned by actual server process")
    return matches[0]


def observe_endpoint(address, process):
    c.require(c.same(c.observe(process["pid"]), process), "endpoint owner changed")
    proc = pathlib.Path(f'/proc/{process["pid"]}')
    table = (proc/"net/tcp").read_text()
    inodes = []
    for path in (proc/"fd").iterdir():
        try: link = os.readlink(path)
        except FileNotFoundError: continue
        if link.startswith("socket:[") and link.endswith("]"): inodes.append(int(link[8:-1]))
    inode = listener_inode(table, inodes, address)
    c.require(c.same(c.observe(process["pid"]), process), "endpoint owner changed during observation")
    return dict(schema_version=1, process=process, address=address, observed_utc=c.now(), proc_net_tcp=table, fd_socket_inodes=inodes, listener_inode=inode)


def endpoint(directory, prefix, owner, address, before):
    value = c.read(directory/(prefix+".endpoint.json"))
    c.require(c.schema(value), "invalid endpoint observation schema")
    c.process(value["process"])
    c.require(c.same(value["process"], owner["process"]) and value["address"] == address, "endpoint process/address mismatch")
    c.require(isinstance(value["fd_socket_inodes"], list) and all(type(v) is int and v > 0 for v in value["fd_socket_inodes"]) and type(value["listener_inode"]) is int, "invalid endpoint socket inode")
    c.require(listener_inode(value["proc_net_tcp"], value["fd_socket_inodes"], address) == value["listener_inode"], "endpoint raw socket evidence mismatch")
    c.require(c.timestamp(owner["observed_utc"]) <= c.timestamp(value["observed_utc"]) <= before, "endpoint observation outside owner/client bounds")


def validate_journal(directory, expected, receipt, manifest):
    journal = [loads(line) for line in (directory/"crash-write.json.acknowledged.jsonl").read_text().splitlines()]
    c.require(len(journal) == 64, "incomplete actual ACK journal")
    previous = c.timestamp(receipt["started_utc"])
    ack = "AckVisible" if manifest["target"] == "treedb_native" else "w:1,j:absent"
    for n, operation in enumerate(journal):
        c.require(c.schema(operation) and type(operation["ordinal"]) is int and operation["ordinal"] == n and operation["id"] == expected["load"][n]["id"] and operation["acknowledged"] is True and operation["requested_ack"] == ack, "failed/ambiguous/unordered crash write")
        bounds = c.result(operation["write"], "actual ordinary-ACK write", bounds=(previous, c.timestamp(receipt["ended_utc"])))
        previous = bounds[1]


def validate(directory):
    directory = pathlib.Path(directory)
    # A deferred cancellation may arrive only after normal reopened cleanup
    # reaped the child. Its parent failure is still terminal for this attempt;
    # never resurrect that otherwise complete packet during later consumption.
    # Presence is enough: malformed files/directories/broken symlinks also fail.
    for location in (directory.parent,directory):
        c.require(not os.path.lexists(location/"failure.json") and not any(location.glob("*.failure.json")), "failed crash attempt cannot qualify")
    m = c.read(directory.parent/"manifest.json")
    c.require(c.schema(m) and m["kind"] == SCOPE and m["replay_proof"] == LIMITATION, "invalid crash control scope")
    c.require(isinstance(m["campaign_id"],str) and m["campaign_id"] and isinstance(m["address"],str), "invalid crash identity/address")
    c.require(m["target"] in ("treedb_native", "treedb_mongo") and m["profile"] in PROFILES and m["format"] in ("json", "bson"), "invalid crash settings")
    c.require(not (m["target"] == "treedb_mongo" and m["format"] == "json"), "Mongo JSON byte shape unsupported")
    build = m["build"]
    c.server_build(build, m["target"])
    c.require(c.same(build, c.read(directory/"server-build.json")), "crash copied build mismatch")
    c.require(c.digest(directory/"server-source.patch") == build["source_patch_sha256"] and (directory/"server-buildinfo.txt").read_text().strip(), "missing crash build provenance")
    c.require(c.schema(m["fixture"]) and c.SHA.fullmatch(m["fixture"]["exe_sha256"]) and c.digest(directory/"fixture-source.go") == m["fixture"]["source_sha256"], "missing crash fixture source/ELF")
    for name, digest in m["contract_sha256"].items():
        c.require(name in ("DURABILITY.md", "COMPATIBILITY.md") and c.digest(directory/name) == digest, "crash source contract mismatch")
    c.require(set(m["contract_sha256"]) == {"DURABILITY.md", "COMPATIBILITY.md"}, "missing source contracts")
    for value in m["deadlines"].values(): c.positive_timeout(value, "crash deadline")
    original = c.read(directory/"server.evidence/owner.open.json")
    c.observation(original, build, m["profile"])
    launch = c.read(directory/"server.launch.json")
    c.process(launch["process"])
    c.require(launch["process"]["exe_sha256"] == build["exe_sha256"] and c.same(launch["process"], original["process"]) and launch["data_dir"] == original["data_dir"], "crash original process/data/ELF mismatch")
    c.require(launch["checkpoint_control_env_absent"] is True, "private checkpoint control enabled before crash")
    db = pathlib.Path(original["data_dir"])
    c.require(db.is_absolute() and db == directory/"db", "crash requires fresh separate task DB")
    for name in ("owner.before-close.json", "owner.close.json", "owner.checkpoint.json", "owner.checkpoint-control.json"):
        c.require(not os.path.lexists(directory/"server.evidence"/name), "original owner graceful close/checkpoint occurred")
    expected = c.read(directory/"expected.json")
    c.require(c.same(expected, canonical_expected(identity(m))), "crash expected intents mismatch")
    write_receipt, write_bounds = helper(directory, "crash-write", m, original)
    endpoint(directory,"server",original,m["address"],write_bounds[0])
    barrier = c.read(directory/"precrash-barrier.json")
    c.require(c.schema(barrier) and barrier["kind"] == "external_evidence_fsync_barrier", "missing external durable evidence barrier")
    c.require(c.same(barrier["synced_directory_paths"], [str(directory),str(directory.parent),str(directory.parent.parent)]), "missing evidence directory publication fsync")
    names = {"expected.json", "crash-write.json", "crash-write.json.acknowledged.jsonl", "crash-write.close.json", "crash-write.launch.json"}
    c.require(set(barrier["artifacts_sha256"]) == names, "incomplete precrash durable artifact set")
    for name, value in barrier["artifacts_sha256"].items():
        c.require(not (directory/name).resolve().is_relative_to(db.resolve()) and value == c.digest(directory/name), "external durable artifact mismatch")
    c.require(write_bounds[1] <= c.timestamp(barrier["start_utc"]) <= c.timestamp(barrier["end_utc"]), "precrash durability ordering mismatch")
    validate_journal(directory, expected, write_receipt, m)
    crash = c.read(directory/"server.crash.json")
    c.require(c.schema(crash) and crash["kind"] == "owned_process_sigkill" and crash["pidfd_pinned"] is True and type(crash["signal"]) is int and crash["signal"] == signal.SIGKILL and type(crash["exit_code"]) is int and crash["exit_code"] == -signal.SIGKILL and crash["reaped"] is True, "missing actual owned process crash/reap")
    c.process(crash["process"])
    c.require(c.same(crash["process"], original["process"]), "crashed process differs from owner")
    c.require(c.timestamp(barrier["end_utc"]) <= c.timestamp(crash["start_utc"]) <= c.timestamp(crash["end_utc"]), "crash precedes durable external ACK evidence")
    stopped = c.read(directory/"server.exit.json")
    c.process(stopped["process"])
    c.require(c.same(stopped["process"], original["process"]) and type(stopped["exit_code"]) is int and stopped["exit_code"] == -signal.SIGKILL, "crash terminal status mismatch")
    c.require(not (directory/"server.failure.json").exists() and not (directory/"reopened.failure.json").exists(), "owned server failure cannot qualify")
    reopened, closed = live.owner(directory, "reopened", build, m["profile"])
    c.require(c.timestamp(crash["end_utc"]) <= c.timestamp(reopened["observed_utc"]), "reopen precedes crash/reap")
    c.require(original["owner_id"] != reopened["owner_id"] and not c.same(original["process"], reopened["process"]), "reopen lacks independent process/owner")
    c.require(all(c.same(original[k], reopened[k]) for k in ("data_dir", "data_device", "data_inode", "backend", "profile")), "reopen storage/version identity mismatch")
    _, verify_bounds = helper(directory, "crash-verify", m, reopened)
    endpoint(directory,"reopened",reopened,m["address"],verify_bounds[0])
    live.require_interval(verify_bounds, live.owner_bounds(directory, "reopened"), "crash verification")
    before = state(expected, c.read(directory/"crash-write.json"), "crash-write")
    after = state(expected, c.read(directory/"crash-verify.json"), "crash-verify")
    counts = validate_state(expected, before, after)
    return dict(schema_version=1, kind=SCOPE, status="PASS", replay_proof=LIMITATION, power_loss_certified=False, **counts)


@contextlib.contextmanager
def owned_server(directory, prefix, command, manifest):
    """Direct-child custody from spawn through pin acquisition and bounded reap."""
    child = descriptor = observed = None
    pending = []
    cleanup_timeout = manifest["deadlines"]["cleanup_timeout_seconds"]
    env = os.environ.copy()
    env.pop("GOMAXPROCS", None)
    env.pop("YCSB_OWNER_CHECKPOINT", None)
    evidence = directory/(prefix+".evidence")
    evidence.mkdir()
    env["YCSB_SERVER_RECEIPT_DIR"] = str(evidence)
    with (directory/(prefix+".out")).open("x") as stdout, (directory/(prefix+".err")).open("x") as stderr:
        try:
            with c.defer_startup_cancellation() as pending:
                child = subprocess.Popen(command, env=env, stdout=stdout, stderr=stderr, start_new_session=True)
                descriptor = os.pidfd_open(child.pid)
            if pending: raise InterruptedError("server startup cancelled: "+str(pending))
            c.observe_owned(child.pid, str(directory/"db"), directory/(prefix+".launch.json"), manifest["build"]["exe_sha256"])
            observed = c.read(directory/(prefix+".launch.json"))["process"]
            proc_env = pathlib.Path(f"/proc/{child.pid}/environ").read_bytes().split(b"\0")
            record = dict(process=observed, data_dir=str(directory/"db"), checkpoint_control_env_absent=not any(v.startswith(b"YCSB_OWNER_CHECKPOINT=") for v in proc_env))
            (directory/(prefix+".launch.json")).write_text(json.dumps(record, allow_nan=False)+"\n")
            live.wait_owner(directory, manifest["target"], prefix, manifest["deadlines"]["readiness_seconds"])
            yield child, descriptor, observed
        except BaseException as error:
            # Failure receipt is retained after cleanup, inside the same shield.
            failure = error
            raise
        finally:
            previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, c.CANCELLATION_SIGNALS)
            try:
                if "failure" in locals() and getattr(signal.getsignal(signal.SIGALRM), "ycsb_outer_deadline", False): signal.setitimer(signal.ITIMER_REAL, 0)
                cleanup = dict(attempted=child is not None, success=False, reaped=False, exit_code=None, error="")
                try:
                    if child is not None:
                        if descriptor is None:
                            code = c.terminate_unpinned_child(child, cleanup_timeout)
                        else:
                            if child.returncode is None: c.terminate_pinned_child(descriptor, cleanup_timeout)
                            code = child.wait(timeout=cleanup_timeout)
                        cleanup.update(success=True, reaped=True, exit_code=code)
                        c.write_new(directory/(prefix+".exit.json"), dict(process=observed, exit_code=code))
                    else: cleanup.update(success=True)
                except BaseException as error:
                    cleanup["error"] = str(error)
                if "failure" in locals() or not cleanup["success"]:
                    c.write_new(directory/(prefix+".failure.json"), dict(schema_version=1, kind="crash_control_owned_server_failure", error=str(locals().get("failure", cleanup["error"])), cleanup=cleanup, pid=child.pid if child else None, pidfd_pinned=descriptor is not None))
                if descriptor is not None: os.close(descriptor)
                c.require(cleanup["success"], "crash control server cleanup failed: "+cleanup["error"])
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)


def crash_owned(child, descriptor, observed, directory, timeout):
    # The pidfd names the direct child; check its observed start/ELF too.
    c.require(not select.select([descriptor], [], [], 0)[0] and c.same(c.observe(child.pid), observed), "owned server identity changed before crash")
    started = c.now()
    signal.pidfd_send_signal(descriptor, signal.SIGKILL)
    code = child.wait(timeout=timeout)
    record = dict(schema_version=1, kind="owned_process_sigkill", process=observed, pidfd_pinned=True, signal=int(signal.SIGKILL), exit_code=code, reaped=True, start_utc=started, end_utc=c.now())
    c.write_new(directory/"server.crash.json", record)
    c.require(code == -signal.SIGKILL, "owned server did not exit from requested crash")


def run(args):
    for path in (args.server_binary,args.fixture_binary):
        c.require(pathlib.Path(path).is_absolute() and os.access(path,os.X_OK), "absolute executable paths required")
    out = args.output.resolve()
    out.mkdir(exist_ok=False)
    args.owned_output = out
    directory = out/args.target
    directory.mkdir()
    (directory/"db").mkdir()
    # Publish fresh evidence directory entries before the service is started.
    artifact_barrier(directory, [], directory/"db")
    server_manifest = c.read(args.server_manifest)
    c.require(c.schema(server_manifest), "invalid server manifest schema")
    build = server_manifest["servers"][args.target]
    c.server_build(build, args.target)
    c.require(c.digest(args.server_binary) == build["exe_sha256"], "server binary mismatch")
    source = pathlib.Path(build["source_dir"])
    c.require(source.is_absolute(), "absolute bound server source directory required")
    def git(*parts): return subprocess.check_output(["git", "-C", str(source), *parts], timeout=10)
    c.require(git("rev-parse", "HEAD").decode().strip() == build["source_commit"] and not git("ls-files", "--others", "--exclude-standard").strip(), "server source unbound")
    patch = git("diff", "--binary", "HEAD")
    c.require(__import__("hashlib").sha256(patch).hexdigest() == build["source_patch_sha256"], "server overlay mismatch")
    fixture = c.read(args.fixture_manifest)
    c.require(c.schema(fixture) and c.digest(args.fixture_binary) == fixture["exe_sha256"] and c.digest(args.fixture_source) == fixture["source_sha256"], "fixture source/ELF mismatch")
    (directory/"server-source.patch").write_bytes(patch)
    (directory/"server-buildinfo.txt").write_bytes(subprocess.check_output([args.go, "version", "-m", args.server_binary], timeout=10))
    (directory/"fixture-source.go").write_bytes(args.fixture_source.read_bytes())
    c.write_new(directory/"server-build.json", build)
    contracts = {}
    for name, original in (("DURABILITY.md", source/"docs/contracts/DURABILITY.md"), ("COMPATIBILITY.md", source/"TreeDB/mongo_gateway/COMPATIBILITY.md")):
        (directory/name).write_bytes(original.read_bytes())
        contracts[name] = c.digest(directory/name)
    manifest = dict(schema_version=1, kind=SCOPE, replay_proof=LIMITATION, campaign_id=uuid.uuid4().hex, target=args.target, profile=args.profile, format=args.format, address=args.address, build=build, server_builds={args.target:build}, fixture=fixture, contract_sha256=contracts, client=dict(build_id="public-process-crash-fixture",exe_sha256=fixture["exe_sha256"]), deadlines=dict(timeout_seconds=args.helper_timeout, cleanup_timeout_seconds=args.cleanup_timeout, outer_timeout_seconds=args.outer_timeout, readiness_seconds=args.readiness_timeout))
    c.write_new(out/"manifest.json", manifest)
    host, port = args.address.rsplit(":", 1)
    c.require(host == "127.0.0.1" and 0 < int(port) < 65536, "crash fixture requires explicit loopback address")
    command = [args.server_binary, "-dir", str(directory/"db"), "-addr", args.address, "-profile", args.profile]
    if args.target == "treedb_mongo": command += ["-document-format", args.format]
    def client(mode):
        command = [args.fixture_binary, "-mode", mode, "-target", "native" if args.target == "treedb_native" else "mongo", "-addr", args.address, "-format", args.format, "-profile", args.profile, "-source-id", build["source_commit"], "-fixture-id", manifest["campaign_id"], "-expected", str(directory/"expected.json"), "-observed", str(directory/(mode+".json")), "-receipt", str(directory/(mode+".close.json"))]
        code = c.launch(argparse.Namespace(directory=str(directory), stem=mode, command=command, timeout=args.helper_timeout, cleanup_timeout=args.cleanup_timeout))
        c.require(code == 0, "public crash helper failed")
    with owned_server(directory, "server", command, manifest) as (child, descriptor, observed):
        wait_tcp(args.address, args.readiness_timeout, observed, directory/"server.endpoint.json")
        client("crash-write")
        # Admission independently validates every actual ACK, before SIGKILL.
        receipt, _ = helper(directory, "crash-write", manifest, c.read(directory/"server.evidence/owner.open.json"))
        expected = c.read(directory/"expected.json")
        c.require(c.same(expected, canonical_expected(identity(manifest))), "crash expected intents mismatch")
        validate_journal(directory, expected, receipt, manifest)
        c.require(c.same(state(expected, c.read(directory/"crash-write.json"), "crash-write"), exact.rows(expected["load"], expected["format"])), "precrash state mismatch")
        names = ["expected.json", "crash-write.json", "crash-write.json.acknowledged.jsonl", "crash-write.close.json", "crash-write.launch.json"]
        barrier = artifact_barrier(directory, names, directory/"db")
        c.write_new(directory/"precrash-barrier.json", barrier)
        artifact_barrier(directory, ["precrash-barrier.json"], directory/"db")
        crash_owned(child, descriptor, observed, directory, args.cleanup_timeout)
    with owned_server(directory, "reopened", command, manifest) as (_, _, observed):
        wait_tcp(args.address, args.readiness_timeout, observed, directory/"reopened.endpoint.json")
        client("crash-verify")
    report = validate(directory)
    c.write_new(directory/"validation.json", report)
    return report


def wait_tcp(address, timeout, process, output):
    host, port = address.rsplit(":", 1)
    deadline = time.monotonic()+timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, int(port)), timeout=min(.1, max(.001, deadline-time.monotonic()))):
                observed = observe_endpoint(address,process)
                c.write_new(output,observed)
                return
        except (OSError,ValueError): time.sleep(.01)
    raise TimeoutError("crash service readiness timed out")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="mode", required=True)
    consume = commands.add_parser("validate")
    consume.add_argument("directory", type=pathlib.Path)
    execute = commands.add_parser("run")
    execute.add_argument("--output", type=pathlib.Path, required=True)
    execute.add_argument("--target", choices=("treedb_native", "treedb_mongo"), required=True)
    execute.add_argument("--profile", choices=PROFILES, required=True)
    execute.add_argument("--format", choices=("json", "bson"), required=True)
    for name in ("server-manifest", "fixture-manifest", "fixture-source"):
        execute.add_argument("--"+name, type=pathlib.Path, required=True)
    for name in ("server-binary", "fixture-binary", "address"): execute.add_argument("--"+name, required=True)
    execute.add_argument("--go", default="go")
    for name, default in (("helper-timeout", 180), ("cleanup-timeout", 10), ("readiness-timeout", 60), ("outer-timeout", 600)):
        execute.add_argument("--"+name, type=float, default=default)
    args = parser.parse_args()
    def interrupted(signum, frame): raise InterruptedError("crash control interrupted: "+str(signum))
    signal.signal(signal.SIGTERM, interrupted)
    try:
        if args.mode == "validate": report = validate(args.directory)
        else:
            c.require(not (args.target == "treedb_mongo" and args.format == "json"), "Mongo JSON byte-valued fixture unsupported")
            for value in (args.helper_timeout, args.cleanup_timeout, args.readiness_timeout, args.outer_timeout): c.positive_timeout(value, "crash deadline")
            with c.outer_deadline(args.outer_timeout): report = run(args)
        print(json.dumps(report, allow_nan=False))
        return 0
    except (KeyboardInterrupt, ValueError, TypeError, KeyError, OSError, subprocess.SubprocessError) as error:
        # Deferred SIGINT raises KeyboardInterrupt after normal cleanup, outside
        # owned_server's exception branch. Retain that top-level failure too.
        previous_mask=signal.pthread_sigmask(signal.SIG_BLOCK,c.CANCELLATION_SIGNALS)
        try:
            message=str(error) or type(error).__name__
            if args.mode == "run" and getattr(args,"owned_output",None) is not None:
                c.write_new(args.owned_output/"failure.json", dict(schema_version=1, kind="public_crash_control_failure", error=message, error_type=type(error).__name__, end_utc=c.now()))
            print("INVALID public crash control: "+message, file=sys.stderr)
            return 3
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK,previous_mask)


if __name__ == "__main__": raise SystemExit(main())
