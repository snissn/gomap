#!/usr/bin/env python3
"""Required live-owner and typed-snapshot contract for the YCSB harness.

These checks consume original observations and helper call receipts. A matching
pair of supplied digest strings or an activation label is never sufficient.
"""
import base64
import hashlib
import json
import os
import pathlib
import re
import signal
import socket
import time
import uuid
from decimal import Decimal, InvalidOperation, localcontext
import ycsb_compare_validate as c
import ycsb_correctness_validate as exact


# Existing monotonic sources only. Pending-debt gauges and maxima are not sums.
GATEWAY_COUNTERS = tuple("ycsb.gateway.write_concern."+name for name in (
    "Requests", "VisibleRequests", "JournalRequests", "LogicalWrites",
    "VisibleAcknowledgements", "JournalAcknowledgements", "SyncAttempts",
    "PhysicalSyncBoundaries", "PreMutationRejections", "MalformedRejections",
    "UnsupportedRejections", "TimeoutRejections", "SyncFailures",
    "DurabilityUnavailableFailures", "SyncBoundaryFailures", "UncertainOutcomes",
    "AcknowledgementNanos", "JournalSyncNanos"))
PHASE_INTEGER_COUNTERS = (
    "treedb.cache.cow.txn.logical_seq", "treedb.cache.checkpoint.runs",
    *("treedb.command_wal."+name for name in (
        "flush.count_total", "flush.ns_total", "sync.count_total", "sync.ns_total",
        "file_sync.calls_total", "file_sync.ns_total", "file_sync.errors_total",
        "directory_sync.calls_total", "directory_sync.ns_total", "directory_sync.errors_total")),
    *("treedb.command_wal.group_commit."+name for name in (
        "attempts_total", "groups_total", "commits_total", "participants_total",
        "dependency_entries_attempted_total", "dependency_entries_covered_total",
        "leaders_total", "followers_total", "syncs_total", "errors_total", "forced_total",
        "wait_ns_total", "trigger.timeout_total", "trigger.commit_limit_total",
        "trigger.byte_limit_total", "trigger.explicit_sync_total", "trigger.sealed_collection_total",
        "bypasses_total")), *GATEWAY_COUNTERS)
PHASE_DECIMAL_COUNTERS = ("treedb.cache.checkpoint.total_ms",)
PHASE_RPC_SECONDS = 5


def phase_stats_enabled(manifest):
    settings = manifest.get("phase_stats")
    if settings is None:
        c.require("phase_stats" not in manifest and "phase_stats_required_counters" not in manifest,
                  "disabled campaign contains phase stats settings")
        return False
    c.require(c.same(settings, dict(schema_version=1, enabled=True, max_observations=64,
                                  rpc_timeout_seconds=PHASE_RPC_SECONDS)), "invalid phase stats contract")
    c.require(manifest["targets"] == ["treedb_mongo"] and
              type(manifest["run_repeats"]) is int and 1 <= manifest["run_repeats"] <= 31,
              "phase stats requires sole gateway target and at most 31 repeats")
    return True


def phase_stats_sequence(stem, boundary):
    c.require(boundary in ("before", "after"), "invalid phase stats boundary")
    c.require(stem == "load" or re.fullmatch(r"run_([1-9]|[12][0-9]|3[01])", stem),
              "invalid phase stats stem")
    return 2*(0 if stem == "load" else int(stem[4:]))+1+(boundary == "after")


def _phase_endpoint(value, opened):
    c.require(c.schema(value) and value["kind"] == "private_owner_checkpoint_control" and
              value["owner_id"] == opened["owner_id"], "phase stats endpoint owner mismatch")
    c.require(isinstance(value["address"], str), "invalid phase stats loopback address")
    address = value["address"].split(":")
    c.require(len(address) == 2 and address[0] == "127.0.0.1" and
              address[1].isascii() and address[1].isdecimal() and 0 < int(address[1]) < 65536,
              "invalid phase stats loopback address")
    return address[0], int(address[1])


def _phase_observation(receipt, request, opened, build, profile, bounds):
    c.require(c.schema(receipt) and receipt["kind"] == "read_only_live_owner_phase_stats" and
              c.same(receipt["request"], request), "phase stats receipt/request mismatch")
    observed = receipt["owner"]
    c.observation(observed, build, profile)
    c.require(observed["backend"] == "cow", "phase stats requires actual native owner")
    for key in ("schema_version", "kind", "owner_id", "process", "data_dir", "data_device",
                "data_inode", "profile", "backend", "observed_gomaxprocs"):
        c.require(c.same(observed[key], opened[key]), "phase stats actual owner identity mismatch")
    interval = c.result(receipt["result"], "read-only phase stats", bounds=bounds)
    c.require(interval[0] <= c.timestamp(observed["observed_utc"]) <= interval[1],
              "phase stats observation outside actual call")
    c.require(not {"collections_flush_all", "collections_checkpoint_storage"} & receipt.keys(),
              "phase stats claims a storage callback")
    return observed


def phase_stats_rpc(directory, stem, boundary, phase_id, manifest):
    """One bounded private observation outside the original client launch clock."""
    c.require(phase_stats_enabled(manifest) and directory.name == "treedb_mongo", "phase stats disabled")
    started, clock = c.now(), time.monotonic_ns()
    deadline = time.monotonic()+PHASE_RPC_SECONDS
    prefix = stem+".phase-stats-"+boundary
    evidence = directory/"server.evidence"
    old_term = signal.getsignal(signal.SIGTERM)
    def cancelled(signum, frame):
        raise InterruptedError("phase stats interrupted by signal "+str(signum))
    signal.signal(signal.SIGTERM, cancelled)
    try:
        build = manifest["server_builds"][directory.name]
        opened = c.read(evidence/"owner.open.json")
        c.observation(opened, build, manifest["profile"])
        c.require(c.same(opened["process"], c.read(directory/"server.launch.json")["process"]) and
                  c.same(opened["process"], c.observe(opened["process"]["pid"])), "stale phase stats owner")
        st = os.stat(opened["data_dir"])
        c.require((st.st_dev, st.st_ino) == (opened["data_device"], opened["data_inode"]),
                  "phase stats data directory changed")
        address = _phase_endpoint(c.read(evidence/"owner.checkpoint-control.json"), opened)
        client = c.observe(os.getpid())
        request = dict(schema_version=1, owner_id=opened["owner_id"], request_id=uuid.uuid4().hex,
                       client=client, source_id=build["source_commit"], operation="phase_stats",
                       campaign_id=manifest["campaign_id"], phase_id=phase_id, stem=stem,
                       boundary=boundary, sequence=phase_stats_sequence(stem, boundary))
        request_path = directory/(prefix+".request.json")
        c.write_new(request_path, request)
        wire = request_path.read_bytes()
        c.require(len(wire) <= 8192, "phase stats request exceeds control framing")
        def remaining():
            seconds = deadline-time.monotonic()
            c.require(seconds > 0, "phase stats 5s RPC budget exhausted")
            return seconds
        with socket.create_connection(address, timeout=remaining()) as connection:
            connection.settimeout(remaining())
            connection.sendall(wire)
            response_bytes = bytearray()
            while b"\n" not in response_bytes and len(response_bytes) <= 1 << 20:
                connection.settimeout(remaining())
                chunk = connection.recv(min(65536, (1 << 20)+1-len(response_bytes)))
                if not chunk:
                    break
                response_bytes.extend(chunk)
        c.require(response_bytes.endswith(b"\n") and len(response_bytes) <= 1 << 20,
                  "phase stats response exceeds framing or is truncated")
        response_path = directory/(prefix+".response.json")
        with response_path.open("xb") as stream:
            stream.write(response_bytes)
        response = c.read(response_path)
        c.require(set(response) == {"receipt", "error"} and response["error"] == "", "phase stats RPC failed: "+str(response.get("error")))
        sequence = request["sequence"]
        copies = {"client_request": request_path, "client_response": response_path}
        for label, name in (("server_request", f"owner.phase-stats-{sequence:02d}-request.json"),
                            ("server_receipt", f"owner.phase-stats-{sequence:02d}.json")):
            copied = directory/(prefix+"."+label.replace("_", "-")+".json")
            with copied.open("xb") as stream:
                stream.write((evidence/name).read_bytes())
            copies[label] = copied
        c.require(c.same(c.read(copies["server_request"]), request) and
                  c.same(c.read(copies["server_receipt"]), response["receipt"]), "phase stats raw/client copies differ")
        c.require(c.same(opened["process"], c.observe(opened["process"]["pid"])), "phase stats owner changed during RPC")
        remaining()
        ended = c.now()
        result = dict(attempted=True, success=True, error="", start_utc=started, end_utc=ended,
                      elapsed_ns=time.monotonic_ns()-clock)
        _phase_observation(response["receipt"], request, opened, build, manifest["profile"],
                           c.result(result, "phase stats RPC"))
        metadata = dict(schema_version=1, kind="owner_phase_stats_client", campaign_id=manifest["campaign_id"],
                        target=directory.name, server_build=build, client=client, phase_id=phase_id,
                        stem=stem, boundary=boundary, sequence=sequence, result=result,
                        artifacts={label: dict(path=p.name, sha256=c.digest(p)) for label,p in copies.items()})
        path = directory/(prefix+".json")
        c.write_new(path, metadata)
        return dict(path=path.name, sha256=c.digest(path), client=client)
    except BaseException as error:
        c.write_new(directory/(prefix+".failure.json"), dict(schema_version=1, kind="owner_phase_stats_failure",
                    campaign_id=manifest["campaign_id"], phase_id=phase_id, stem=stem, boundary=boundary,
                    start_utc=started, end_utc=c.now(), error=str(error)))
        raise
    finally:
        signal.signal(signal.SIGTERM, old_term)


def phase_counter_deltas(before, after, required=()):
    inventory = set(PHASE_INTEGER_COUNTERS+PHASE_DECIMAL_COUNTERS)
    c.require(isinstance(required, list) or required == (), "invalid required phase counter list")
    c.require(all(isinstance(name, str) for name in required) and len(set(required)) == len(required),
              "invalid/duplicate required phase counter")
    c.require(set(required) <= inventory, "unknown required phase counter")
    required = set(required) | set(GATEWAY_COUNTERS) | {"treedb.cache.cow.txn.logical_seq"}
    values = {}
    for name in PHASE_INTEGER_COUNTERS+PHASE_DECIMAL_COUNTERS:
        parsed = []
        for snapshot in (before, after):
            if name not in snapshot:
                parsed.append(None)
                continue
            value = snapshot[name]
            c.require(isinstance(value, str), "non-string phase counter: "+name)
            if name in PHASE_INTEGER_COUNTERS:
                c.require(re.fullmatch(r"[0-9]+", value) is not None and int(value) <= (1 << 64)-1,
                          "invalid unsigned phase counter: "+name)
                parsed.append(int(value))
            else:
                try:
                    number = Decimal(value)
                except InvalidOperation:
                    raise ValueError("invalid decimal phase counter: "+name)
                c.require(number.is_finite() and number >= 0, "nonfinite/negative phase counter: "+name)
                parsed.append(number)
        c.require(name not in required or None not in parsed, "required phase counter unavailable: "+name)
        if None in parsed:
            values[name] = dict(available=False)
        else:
            c.require(parsed[1] >= parsed[0], "phase counter decreased/reset: "+name)
            if name in PHASE_DECIMAL_COUNTERS:
                # Preserve every decimal digit; the default 28-digit context
                # would silently round large cumulative counters.
                with localcontext() as context:
                    exponent = min(value.as_tuple().exponent for value in parsed)
                    context.prec = max(len(value.as_tuple().digits)+value.as_tuple().exponent-exponent for value in parsed)+1
                    delta = str(parsed[1]-parsed[0])
            else:
                delta = str(parsed[1]-parsed[0])
            values[name] = dict(available=True, before=before[name], after=after[name], delta=delta)
    return values


def phase_stats_boundary(directory, stem, boundary, launch, manifest):
    build = manifest["server_builds"][directory.name]
    evidence = directory/"server.evidence"
    opened = c.read(evidence/"owner.open.json")
    c.observation(opened, build, manifest["profile"])
    _phase_endpoint(c.read(evidence/"owner.checkpoint-control.json"), opened)
    prefix = stem+".phase-stats-"+boundary
    binding = launch["phase_stats"][boundary]
    c.require(set(binding) == {"path", "sha256", "client"} and binding["path"] == prefix+".json" and binding["sha256"] == c.digest(directory/binding["path"]),
              "phase stats metadata binding mismatch")
    metadata = c.read(directory/binding["path"])
    sequence = phase_stats_sequence(stem, boundary)
    c.require(c.schema(metadata) and metadata["kind"] == "owner_phase_stats_client" and
              metadata["campaign_id"] == manifest["campaign_id"] and metadata["target"] == directory.name and
              c.same(metadata["server_build"], build) and metadata["phase_id"] == launch["phase_id"] and
              metadata["stem"] == stem and metadata["boundary"] == boundary and
              type(metadata["sequence"]) is int and metadata["sequence"] == sequence,
              "phase stats metadata identity mismatch")
    c.process(metadata["client"])
    c.require(c.same(binding["client"], metadata["client"]), "phase stats client binding mismatch")
    c.require(not (directory/(prefix+".failure.json")).exists(), "failed phase stats observation")
    copies = {}
    c.require(set(metadata["artifacts"]) == {"client_request", "client_response", "server_request", "server_receipt"},
              "phase stats artifact inventory mismatch")
    for label, suffix in (("client_request", ".request.json"), ("client_response", ".response.json"),
                          ("server_request", ".server-request.json"), ("server_receipt", ".server-receipt.json")):
        item = metadata["artifacts"][label]
        c.require(item["path"] == prefix+suffix and item["sha256"] == c.digest(directory/item["path"]),
                  "phase stats artifact hash/path mismatch")
        copies[label] = c.read(directory/item["path"])
    request, response = copies["client_request"], copies["client_response"]
    c.require(c.same(request, copies["server_request"]) and c.same(response["receipt"], copies["server_receipt"]) and
              set(response) == {"receipt", "error"} and response["error"] == "", "phase stats raw/client mismatch")
    for label, name in (("server_request", f"owner.phase-stats-{sequence:02d}-request.json"),
                        ("server_receipt", f"owner.phase-stats-{sequence:02d}.json")):
        original = evidence/name
        copied = directory/metadata["artifacts"][label]["path"]
        c.require(original.read_bytes() == copied.read_bytes(), "phase stats original/copy byte mismatch")
    c.require(set(request) == {"schema_version", "owner_id", "request_id", "client", "source_id", "operation",
                               "campaign_id", "phase_id", "stem", "boundary", "sequence"}, "unknown/missing phase stats request field")
    c.require(isinstance(request["phase_id"], str) and re.fullmatch(
        re.escape(manifest["campaign_id"]+":treedb_mongo:"+stem+":")+r"[0-9a-f]{32}", request["phase_id"]),
        "invalid phase stats launcher phase identity")
    c.require(c.schema(request) and request["operation"] == "phase_stats" and request["owner_id"] == opened["owner_id"] and
              isinstance(request["request_id"], str) and re.fullmatch(r"[0-9a-f]{32}", request["request_id"]) and
              c.same(request["client"], metadata["client"]) and request["source_id"] == build["source_commit"] and
              request["campaign_id"] == manifest["campaign_id"] and request["phase_id"] == launch["phase_id"] and
              request["stem"] == stem and request["boundary"] == boundary and
              type(request["sequence"]) is int and request["sequence"] == sequence and
              not request.get("fixture_id") and not request.get("snapshot_sha256"), "phase stats request identity mismatch")
    bounds = c.result(metadata["result"], "phase stats RPC", bounds=owner_bounds(directory, "server"))
    c.require(bounds[1]-bounds[0] <= PHASE_RPC_SECONDS*1_000_000_000 and metadata["result"]["elapsed_ns"] <= PHASE_RPC_SECONDS*1_000_000_000,
              "phase stats RPC exceeded 5s")
    observed = _phase_observation(response["receipt"], request, opened, build, manifest["profile"], bounds)
    return dict(bounds=bounds, observation=observed, request=request)


def phase_stats_campaign(directory, manifest):
    enabled = phase_stats_enabled(manifest)
    stems = ["load"]+[f"run_{i}" for i in range(1, manifest["run_repeats"]+1)]
    secondary_claims = [path for owner in ("reopened", "fixture-write", "fixture-reopened")
                        for path in (directory/(owner+".evidence")).glob("owner.phase-stats-*.json")]
    c.require(not secondary_claims, "secondary owners contain phase stats claims")
    claims = list(directory.glob("*.phase-stats-*.json"))+list((directory/"server.evidence").glob("owner.phase-stats-*.json"))
    if not enabled:
        c.require(not claims and not any("phase_stats" in c.read(directory/(stem+".launch.json")) for stem in stems),
                  "disabled campaign contains phase stats claims")
        return []
    rows, nonces, previous = [], set(), None
    previous_stats = None
    for stem in stems:
        launch = c.read(directory/(stem+".launch.json"))
        c.require(set(launch["phase_stats"]) == {"before", "after"}, "incomplete phase stats pair")
        before, after = [phase_stats_boundary(directory, stem, boundary, launch, manifest) for boundary in ("before", "after")]
        c.require(c.same(before["request"]["client"], after["request"]["client"]), "phase stats launcher changed within phase")
        c.require(before["bounds"][1] <= c.timestamp(launch["start_utc"]) <= c.timestamp(launch["end_utc"]) <= after["bounds"][0],
                  "phase stats overlaps original client launch clock")
        c.require(previous is None or previous <= before["bounds"][0], "phase stats observations overlap/out of order")
        previous = after["bounds"][1]
        required = manifest.get("phase_stats_required_counters", [])
        if previous_stats is not None:
            # Census, sentinel and maintenance can add work between phases;
            # they cannot reset an original owner's cumulative counters.
            phase_counter_deltas(previous_stats, before["observation"]["stats"], required)
        previous_stats = after["observation"]["stats"]
        for item in (before, after):
            nonce = item["request"]["request_id"]
            c.require(nonce not in nonces, "reused phase stats request nonce")
            nonces.add(nonce)
        rows.append(dict(stem=stem, before=before, after=after,
                         counters=phase_counter_deltas(before["observation"]["stats"], after["observation"]["stats"],
                                                       required)))
    expected = {f"owner.phase-stats-{i:02d}{suffix}.json" for i in range(1, 2*len(stems)+1) for suffix in ("", "-request")}
    c.require({p.name for p in (directory/"server.evidence").glob("owner.phase-stats-*.json")} == expected,
              "phase stats sequence inventory mismatch")
    expected_clients = {stem+".phase-stats-"+boundary+suffix+".json" for stem in stems
                        for boundary in ("before", "after")
                        for suffix in ("", ".request", ".response", ".server-request", ".server-receipt")}
    c.require({p.name for p in directory.glob("*.phase-stats-*.json")} == expected_clients,
              "phase stats client artifact inventory mismatch")
    return rows


def expected_keys(count):
    # Accepted go-ycsb util.Hash64 + workload.buildKeyName, insertorder=hashed,
    # keyprefix=user, zeropadding=1, insertstart=0 (all explicit launcher inputs).
    result = set()
    for number in range(count):
        value = 14695981039346656037
        for byte in number.to_bytes(8, "big"):
            value = ((value ^ byte) * 1099511628211) & ((1 << 64)-1)
        signed = value if value < (1 << 63) else value-(1 << 64)
        absolute = -signed if signed < 0 and signed != -(1 << 63) else signed
        result.add("user"+str(absolute))
    c.require(len(result) == count, "hashed key collision")
    return result


def canonical_snapshot(value, manifest, target, stage):
    c.require(c.schema(value) and value["source_id"] == manifest["server_builds"][target]["source_commit"], "random snapshot source mismatch")
    c.require(value["fixture_id"] == manifest["campaign_id"]+":"+target+":random", "random snapshot campaign mismatch")
    c.require(value["profile"] == manifest["profile"] and value["format"] == manifest["formats"][target] and value["target"] == ({"treedb_native":"native","treedb_mongo":"mongo","mongodb":"mongodb"}[target]), "random snapshot settings mismatch")
    c.require(value["stage"] == "random_"+stage and value["operations"] == [], "invalid random snapshot stage")
    rows = exact.rows(value["rows"], value["format"])
    c.require(set(rows) == expected_keys(manifest["recordcount"]), "random snapshot key set mismatch")
    for fields in rows.values():
        c.require(set(fields) == {"field"+str(i) for i in range(10)}, "random snapshot field set mismatch")
        for typed in fields.values():
            raw = typed["value"] if value["format"] == "json" else typed["base64"]
            c.require(len(base64.b64decode(raw, validate=True)) == 100, "random snapshot field length mismatch")
    canonical = json.dumps(rows, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return rows, hashlib.sha256(canonical).hexdigest()


def owner(directory, prefix, build, profile):
    launch = c.read(directory/(prefix+".launch.json"))
    stopped = c.read(directory/(prefix+".exit.json"))
    c.process(launch["process"])
    c.require(launch["process"]["exe_sha256"] == build["exe_sha256"], "live owner ELF mismatch")
    c.process(stopped["process"])
    c.exit_success(stopped["exit_code"],"live owner checked exit missing")
    c.require(c.same(stopped["process"],launch["process"]), "live owner checked exit missing")
    evidence = directory/(prefix+".evidence")
    opened, before, closed = [c.read(evidence/name) for name in ("owner.open.json", "owner.before-close.json", "owner.close.json")]
    for observation in (opened,before):
        c.observation(observation,build,profile)
        c.require(c.same(observation["process"],launch["process"]) and observation["data_dir"] == launch["data_dir"], "raw owner process/data mismatch")
    for key in ("owner_id","process","data_dir","data_device","data_inode","profile","backend"):
        c.require(c.same(before[key],opened[key]), "live owner identity changed")
    c.require(c.schema(closed) and closed["kind"] == "original_server_owner_close", "original owner Close binding mismatch")
    c.process(closed["process"])
    c.require(closed["owner_id"] == opened["owner_id"] and c.same(closed["process"],launch["process"]), "original owner Close binding mismatch")
    bounds = c.result(closed["close"], "original owner Close")
    c.require(c.timestamp(opened["observed_utc"]) <= c.timestamp(before["observed_utc"]) <= bounds[0], "raw owner ordering mismatch")
    return opened,closed


def owner_bounds(directory,prefix):
    evidence=directory/(prefix+".evidence")
    return (c.timestamp(c.read(evidence/"owner.open.json")["observed_utc"]),
            c.timestamp(c.read(evidence/"owner.before-close.json")["observed_utc"]))


def require_interval(bounds,lifetime,label):
    c.require(lifetime[0] <= bounds[0] <= bounds[1] <= lifetime[1],label+" outside complete observed owner lifetime")


def helper(directory, stem, manifest, target, mode, fixture_id):
    launch,receipt = [c.read(directory/(stem+suffix)) for suffix in (".launch.json",".close.json")]
    c.require(c.schema(launch), "unsupported helper launch schema")
    c.process(launch["process"])
    c.exit_success(launch["exit_code"],"snapshot/fixture child failure or stale campaign")
    c.require(launch["campaign_id"] == manifest["campaign_id"] and launch.get("timed_out") is not True and not (directory/(stem+".failure.json")).exists(), "snapshot/fixture child failure or stale campaign")
    c.require(launch["process"]["exe_sha256"] == manifest["fixture"]["exe_sha256"], "snapshot/fixture ELF mismatch")
    c.require(launch["receipt_sha256"] == c.digest(directory/(stem+".close.json")), "snapshot/fixture receipt hash mismatch")
    c.require(c.schema(receipt) and receipt["kind"] == ("random_snapshot_client_close" if mode == "snapshot" else "deterministic_fixture_client_close"), "wrong snapshot/fixture receipt kind")
    c.require(type(receipt["pid"]) is int and receipt["pid"]>0, "snapshot/fixture PID must be an integer")
    c.require(receipt["source_id"] == manifest["server_builds"][target]["source_commit"] and receipt["fixture_id"] == fixture_id and receipt["mode"] == mode and receipt["pid"] == launch["process"]["pid"], "snapshot/fixture process/source mismatch")
    c.require(receipt["result_error"] == "" and receipt["close_error"] == "", "snapshot/fixture original client close failed")
    c.require(c.same_timeout(launch["timeout_seconds"],manifest["deadlines"]["helper_seconds"]) and launch["timed_out"] is False and c.same_timeout(launch["cleanup_timeout_seconds"],manifest["deadlines"]["cleanup_seconds"]) and c.same_timeout(launch["outer_timeout_seconds"],manifest["deadlines"]["helper_outer_seconds"]),"helper deadline binding mismatch")
    bounds = c.timestamp(launch["start_utc"]),c.timestamp(launch["end_utc"])
    c.require(bounds[0] <= c.timestamp(receipt["started_utc"]) <= c.timestamp(receipt["ended_utc"]) <= bounds[1], "snapshot/fixture call interval mismatch")
    c.require(type(receipt["checkpoint_attempted"]) is bool and type(receipt["checkpoint_succeeded"]) is bool,"invalid checkpoint flags")
    if receipt.get("checkpoint") is not None:
        c.result(receipt["checkpoint"],"helper checkpoint",bounds=(c.timestamp(receipt["started_utc"]),c.timestamp(receipt["ended_utc"])))
    if target=="treedb_native" and mode=="write":
        c.require(receipt["checkpoint_attempted"] is True and receipt["checkpoint_succeeded"] is True,"fixture checkpoint failed/skipped")
        c.result(receipt["checkpoint"],"native deterministic checkpoint",bounds=(c.timestamp(receipt["started_utc"]),c.timestamp(receipt["ended_utc"])))

    for name,hash_value in receipt["artifacts_sha256"].items():
        c.require(pathlib.Path(name).name == name and hash_value == c.digest(directory/name), "snapshot/fixture artifact hash mismatch")
    required = {stem+".json"} if mode == "snapshot" else {"fixture.expected.json", "fixture.updated.json", "fixture.updated.json.load.json"} if mode == "write" else {"fixture.expected.json", "fixture.reopened.json"}
    c.require(required <= receipt["artifacts_sha256"].keys(), "missing snapshot/fixture artifact binding")
    return receipt,bounds


def checkpoint_request(value):
    c.require(c.schema(value),"checkpoint request schema mismatch")
    c.require(isinstance(value["owner_id"],str) and re.fullmatch(r"[0-9a-f]{32}",value["owner_id"]),"checkpoint request owner mismatch")
    c.require(isinstance(value["request_id"],str) and re.fullmatch(r"[0-9a-f]{32}",value["request_id"]),"checkpoint request owner mismatch")
    c.process(value["client"])
    c.require(all(isinstance(value[k],str) and value[k] for k in ("source_id","fixture_id")) and isinstance(value["snapshot_sha256"],str) and c.SHA.fullmatch(value["snapshot_sha256"]),"checkpoint request source/snapshot missing")


def checkpoint_proof(value,opened,bounds):
    c.require(isinstance(value,dict) and type(value.get("schema_version")) is int and value["schema_version"]==2 and value["kind"]=="explicit_live_owner_checkpoint","missing raw explicit owner checkpoint")
    checkpoint_request(value["request"])
    observed=value["owner"]
    c.observation(observed,{"expected_owner":opened["backend"]},opened["profile"])
    for key in ("schema_version","kind","owner_id","process","data_dir","data_device","data_inode","profile","backend"):
        c.require(c.same(observed[key],opened[key]),"checkpoint actual owner identity mismatch")
    c.require(c.same(observed["observed_gomaxprocs"],opened["observed_gomaxprocs"]),"checkpoint live owner runtime/profile mismatch")
    overall=c.result(value["result"],"owner checkpoint result",bounds=bounds)
    flush=c.result(value["collections_flush_all"],"owner collections FlushAll",bounds=overall)
    checkpoint=c.result(value["collections_checkpoint_storage"],"owner collections CheckpointStorage",bounds=overall)
    c.require(overall[0] <= c.timestamp(observed["observed_utc"]) <= flush[0] <= flush[1] <= checkpoint[0] <= checkpoint[1] <= overall[1],"explicit owner checkpoint call ordering mismatch")
    return overall


def owner_checkpoint(directory,prefix,stem,receipt,bounds,opened,source_id,fixture_id,snapshot):
    evidence=directory/(prefix+".evidence")
    endpoint=c.read(evidence/"owner.checkpoint-control.json")
    c.require(c.schema(endpoint) and endpoint["kind"]=="private_owner_checkpoint_control" and endpoint["owner_id"]==opened["owner_id"],"checkpoint control endpoint owner mismatch")
    c.require(isinstance(endpoint["address"],str),"invalid private checkpoint control address")
    address=endpoint["address"].split(":")
    c.require(len(address)==2 and address[0]=="127.0.0.1" and address[1].isascii() and address[1].isdecimal() and 0<int(address[1])<65536,"invalid private checkpoint control address")
    raw=c.read(evidence/"owner.checkpoint.json")
    request=c.read(evidence/"owner.checkpoint-request.json")
    checkpoint_request(request)
    for proof in (raw,receipt["owner_checkpoint"]):
        result=checkpoint_proof(proof,opened,bounds)
        require_interval(result,owner_bounds(directory,prefix),"explicit owner checkpoint")
    c.require(c.same(raw,receipt["owner_checkpoint"]) and c.same(raw["request"],request),"checkpoint raw/client/request mismatch")
    launch=c.read(directory/(stem+".launch.json"))
    c.process(launch["process"])
    c.require(request["owner_id"]==opened["owner_id"] and c.same(request["client"],launch["process"]) and request["source_id"]==source_id and request["fixture_id"]==fixture_id and request["snapshot_sha256"]==c.digest(directory/snapshot),"checkpoint request client/source/snapshot mismatch")
    c.result(receipt["checkpoint"],"helper checkpoint aggregate",bounds=bounds)
    c.require(c.same(receipt["checkpoint"],raw["result"]) and receipt["checkpoint_attempted"] is True and receipt["checkpoint_succeeded"] is True,"checkpoint aggregate differs from actual owner calls")


def validate(directory, target, manifest, lifecycle):
    c.require(type(manifest.get("lifecycle_contract")) is int and manifest["lifecycle_contract"] == 2, "required live lifecycle contract 2 missing")
    c.require(target != "mongodb", "external Mongo live owner/checkpoint evidence is unsupported")
    fixture = manifest["fixture"]
    c.require(c.schema(fixture) and c.SHA.fullmatch(fixture["exe_sha256"]) and c.SHA.fullmatch(fixture["source_sha256"]), "unbound fixture source/ELF")
    c.require(c.digest(directory.parent/"fixture-source.go") == fixture["source_sha256"], "fixture source mismatch")
    build = manifest["server_builds"][target]
    c.require(c.SHA.fullmatch(build["source_patch_sha256"]) and c.digest(directory/"server-source.patch") == build["source_patch_sha256"], "server instrumentation source overlay mismatch")
    c.require((directory/"server-buildinfo.txt").read_text().strip(), "missing actual server buildinfo")
    original, closed = owner(directory,"server",build,manifest["profile"])
    reopened, reopened_close = owner(directory,"reopened",build,manifest["profile"])
    runtime=c.read(directory/"runtime.json")
    for aggregate in (runtime,lifecycle):
        c.require(c.schema(aggregate),"invalid aggregate schema")
        c.process(aggregate["process"])
        c.require(c.same(aggregate["process"],original["process"]),"aggregate owner process mismatch")
    c.exit_success(lifecycle["server_exit_code"],"server close exit failure")
    c.require(type(runtime["observed_gomaxprocs"]) is int and c.same(runtime["observed_gomaxprocs"],original["observed_gomaxprocs"]), "aggregate runtime differs from actual observation")
    c.require(original["owner_id"] != reopened["owner_id"] and original["process"] != reopened["process"], "reopen must have independent process/owner identity")
    c.require(all(original[k] == reopened[k] for k in ("data_dir","data_device","data_inode")), "reopen data directory identity mismatch")
    c.require(c.timestamp(closed["close"]["end_utc"]) <= c.timestamp(reopened["observed_utc"]), "reopen precedes original Close")
    snapshots = {}
    receipts = {}
    random_id = manifest["campaign_id"]+":"+target+":random"
    for stage in ("loaded","preclose","reopened"):
        receipt,bounds = helper(directory,"snapshot_"+stage,manifest,target,"snapshot",random_id)
        receipts[stage] = (receipt,bounds)
        snapshots[stage] = canonical_snapshot(c.read(directory/("snapshot_"+stage+".json")),manifest,target,stage)
        require_interval(bounds,owner_bounds(directory,"reopened" if stage=="reopened" else "server"),"snapshot")
    load_launch=c.read(directory/"load.launch.json")
    runs=[c.read(directory/("run_"+str(i)+".launch.json")) for i in range(1,manifest["run_repeats"]+1)]
    measured=[load_launch]+runs
    previous_end=None
    for index,launch in enumerate(measured):
        bounds=(c.timestamp(launch["start_utc"]),c.timestamp(launch["end_utc"]))
        require_interval(bounds,owner_bounds(directory,"server"),"measured phase")
        c.require(previous_end is None or previous_end <= bounds[0],"measured phases overlap/out of order")
        previous_end=bounds[1]
    c.require(c.timestamp(load_launch["end_utc"]) <= receipts["loaded"][1][0] <= receipts["loaded"][1][1] <= c.timestamp(runs[0]["start_utc"]), "loaded snapshot is not between load and measured runs")
    c.require(max(c.timestamp(value["end_utc"]) for value in runs) <= receipts["preclose"][1][0], "preclose snapshot precedes settled measured clients")
    phase_rows = phase_stats_campaign(directory, manifest)
    if phase_rows:
        c.require(phase_rows[0]["after"]["bounds"][1] <= receipts["loaded"][1][0] <=
                  receipts["loaded"][1][1] <= phase_rows[1]["before"]["bounds"][0],
                  "loaded census overlaps phase stats brackets")
        c.require(phase_rows[-1]["after"]["bounds"][1] <= receipts["preclose"][1][0],
                  "preclose helper overlaps phase stats bracket")
        if "common_load" in manifest:
            sentinel = c.read(directory/"public-sentinel.json")
            c.require(receipts["loaded"][1][1] <= c.timestamp(sentinel["start_utc"]) <=
                      c.timestamp(sentinel["end_utc"]) <= phase_rows[1]["before"]["bounds"][0],
                      "common-load sentinel overlaps phase stats bracket")
    c.require(c.same(snapshots["preclose"][0],snapshots["reopened"][0]), "canonical typed random state preservation mismatch")
    c.require(lifecycle["preclose_state_sha256"] == snapshots["preclose"][1] and lifecycle["reopened_state_sha256"] == snapshots["reopened"][1], "canonical random snapshot digest mismatch")
    c.require(lifecycle["preclose_state_utc"] == receipts["preclose"][0]["ended_utc"] and lifecycle["reopened_state_utc"] == receipts["reopened"][0]["ended_utc"], "aggregate snapshot timestamps differ from actual calls")
    reopen_bounds=c.result(lifecycle["reopen"],"reopen observation interval")
    c.require(lifecycle["reopen"]["start_utc"] == closed["close"]["end_utc"] and lifecycle["reopen"]["end_utc"] == reopened["observed_utc"],"aggregate reopen bounds differ from observed owners")
    preservation=c.result(lifecycle["random_state_preservation"],"random state preservation interval")
    c.require(receipts["reopened"][1][1] <= preservation[0],"state comparison precedes completed reopened snapshot")
    checkpoint = receipts["preclose"][0]
    c.require(checkpoint["checkpoint_attempted"] is True and checkpoint["checkpoint_succeeded"] is True, "missing real explicit checkpoint call")
    c.result(checkpoint["checkpoint"],"actual explicit checkpoint",bounds=receipts["preclose"][1])
    if target=="treedb_mongo":
        owner_checkpoint(directory,"server","snapshot_preclose",checkpoint,receipts["preclose"][1],original,build["source_commit"],random_id,"snapshot_preclose.json")
    c.require(c.same(lifecycle["checkpoint"],checkpoint["checkpoint"]) and c.same(lifecycle["server_close"],closed["close"]), "aggregate lifecycle differs from actual calls")
    c.process(lifecycle["reopened_process"])
    c.require(lifecycle["owner_id"] == original["owner_id"] and c.same(lifecycle["reopened_process"],reopened["process"]) and lifecycle["reopened_owner_id"] == reopened["owner_id"], "aggregate reopened owner mismatch")
    # Independently named deterministic fixture uses a fresh database and two
    # independently observed server owners; it cannot stand in for random state.
    first,first_close = owner(directory,"fixture-write",build,manifest["profile"])
    second,second_close = owner(directory,"fixture-reopened",build,manifest["profile"])
    c.require(c.timestamp(reopened_close["close"]["end_utc"]) <= c.timestamp(first["observed_utc"]),"fixture owner overlaps random reopened owner lifetime")
    owners=(original,reopened,first,second)
    c.require(len({item["owner_id"] for item in owners}) == 4 and len({(item["process"]["pid"],item["process"]["start_ticks"]) for item in owners}) == 4,"all service lifetimes require independent owners/processes")
    c.require(first["data_dir"] != original["data_dir"] and first["owner_id"] != second["owner_id"] and first["process"] != second["process"], "fixture requires fresh data and independent reopen")
    c.require(all(first[k] == second[k] for k in ("data_dir","data_device","data_inode")), "fixture reopened data mismatch")
    c.require(c.timestamp(first_close["close"]["end_utc"]) <= c.timestamp(second["observed_utc"]), "fixture reopen precedes original Close")
    fixture_id = manifest["campaign_id"]+":"+target+":deterministic"
    for stem,mode,opened,closing in (("fixture_write","write",first,first_close),("fixture_verify","verify",second,second_close)):
        receipt,bounds=helper(directory,stem,manifest,target,mode,fixture_id)
        require_interval(bounds,owner_bounds(directory,"fixture-write" if mode=="write" else "fixture-reopened"),"fixture call")
        if target=="treedb_mongo" and mode=="write":
            owner_checkpoint(directory,"fixture-write","fixture_write",receipt,bounds,first,build["source_commit"],fixture_id,"fixture.updated.json")
        if target == "treedb_native" and mode == "write":
            c.require(receipt["checkpoint_attempted"] is True and receipt["checkpoint_succeeded"] is True,"fixture checkpoint failed/skipped")
    expected = c.read(directory/"fixture.expected.json")
    c.require(expected["source_id"] == build["source_commit"] and expected["fixture_id"] == fixture_id and expected["format"] == manifest["formats"][target] and expected["profile"] == manifest["profile"], "deterministic fixture identity mismatch")
    exact.validate(expected,[c.read(directory/name) for name in ("fixture.updated.json.load.json","fixture.updated.json","fixture.reopened.json")])
    return phase_rows



def wait_owner(directory,target,prefix,timeout):
    """Bound client launch on actual matching owner evidence, independently of TCP."""
    import time
    c.positive_timeout(timeout,"owner readiness timeout")
    manifest=c.read(directory.parent/"manifest.json")
    build=manifest["server_builds"][target]
    c.server_build(build,target)
    launch=c.read(directory/(prefix+".launch.json"))
    c.process(launch["process"])
    path=directory/(prefix+".evidence/owner.open.json")
    deadline=time.monotonic()+timeout
    while True:
        c.require(c.same(c.observe(launch["process"]["pid"]),launch["process"]),"owner changed before readiness")
        if time.monotonic()>=deadline:raise TimeoutError("timed out waiting for complete matching owner.open observation")
        try:
            observed=c.read(path)
            break
        except (FileNotFoundError,json.JSONDecodeError,UnicodeDecodeError):
            # writeNew creates exclusively before writing. Existence alone is
            # not publication; retry partial JSON within the same bounded live
            # process identity. Complete but invalid schema/data fails below.
            time.sleep(min(.05,max(0,deadline-time.monotonic())))
    c.observation(observed,build,manifest["profile"])
    c.require(c.same(observed["process"],launch["process"]) and c.same(c.observe(launch["process"]["pid"]),launch["process"]),"readiness owner process mismatch")
    c.require(observed["process"]["exe_sha256"]==build["exe_sha256"] and observed["data_dir"]==launch["data_dir"],"readiness owner ELF/data mismatch")
    data=pathlib.Path(launch["data_dir"]).stat()
    c.require(observed["data_device"]==data.st_dev and observed["data_inode"]==data.st_ino,"readiness owner data inode mismatch")
    c.require(c.timestamp(observed["observed_utc"])<=c.timestamp(c.now()),"readiness observation is in the future")
    c.write_new(directory/(prefix+".ready.json"),dict(schema_version=1,process=observed["process"],owner_id=observed["owner_id"],observation_sha256=c.digest(path),ready_utc=c.now()))


def run_helper(directory,target,addr,mode,stage=None,timeout=180,cleanup_timeout=10):
    import argparse
    manifest=c.read(directory.parent/"manifest.json")
    stem="snapshot_"+stage if mode=="snapshot" else "fixture_"+("write" if mode=="write" else "verify")
    identity=manifest["campaign_id"]+":"+target+(":random" if mode=="snapshot" else ":deterministic")
    observed=directory/(stem+".json") if mode=="snapshot" else directory/("fixture.updated.json" if mode=="write" else "fixture.reopened.json")
    command=[manifest["fixture"]["binary"],"-mode",mode,"-target","native" if target=="treedb_native" else "mongo","-addr",addr,"-format",manifest["formats"][target],"-profile",manifest["profile"],"-source-id",manifest["server_builds"][target]["source_commit"],"-fixture-id",identity,"-expected",str(directory/"fixture.expected.json"),"-observed",str(observed),"-receipt",str(directory/(stem+".close.json"))]
    if mode=="snapshot":
        command += ["-stage",stage]
        if stage=="preclose": command += ["-checkpoint"]
    if target=="treedb_mongo" and (mode=="write" or (mode=="snapshot" and stage=="preclose")):
        command += ["-owner-checkpoint-dir",str(directory/("fixture-write.evidence" if mode=="write" else "server.evidence"))]
    code=c.launch(argparse.Namespace(directory=str(directory),stem=stem,command=command,timeout=timeout,cleanup_timeout=cleanup_timeout,role="fixture"))
    c.require(code==0,"snapshot/fixture child failed with exit "+str(code))


def interval(start,end):
    return dict(attempted=True,success=True,error="",start_utc=start,end_utc=end,elapsed_ns=c.timestamp(end)-c.timestamp(start))


def assemble(directory,target):
    manifest=c.read(directory.parent/"manifest.json")
    build=manifest["server_builds"][target]
    opened,closed=owner(directory,"server",build,manifest["profile"])
    reopened,_=owner(directory,"reopened",build,manifest["profile"])
    ids=[c.read(directory/(stem+".launch.json"))["phase_id"] for stem in ["load"]+["run_"+str(i) for i in range(1,manifest["run_repeats"]+1)]]
    shared=dict(schema_version=1,version=build["version"],source_commit=build["source_commit"],campaign_id=manifest["campaign_id"],target=target,profile=manifest["profile"],format=manifest["formats"][target],process=opened["process"],data_dir=opened["data_dir"],owner_id=opened["owner_id"],phase_ids=ids)
    c.write_new(directory/"runtime.json",dict(**shared,observed_gomaxprocs=opened["observed_gomaxprocs"],backend=opened["backend"],observation="server.evidence/owner.open.json (actual live backend and procfs)"))
    before_receipt=c.read(directory/"snapshot_preclose.close.json")
    after_receipt=c.read(directory/"snapshot_reopened.close.json")
    started=c.now()
    before,before_digest=canonical_snapshot(c.read(directory/"snapshot_preclose.json"),manifest,target,"preclose")
    after,after_digest=canonical_snapshot(c.read(directory/"snapshot_reopened.json"),manifest,target,"reopened")
    c.require(c.same(before,after),"canonical typed random state preservation mismatch")
    ended=c.now()
    lifecycle=dict(**shared,server_exit_code=0,checkpoint=before_receipt["checkpoint"],server_close=closed["close"],reopen=interval(closed["close"]["end_utc"],reopened["observed_utc"]),random_state_preservation=interval(started,ended),preclose_state_utc=before_receipt["ended_utc"],reopened_state_utc=after_receipt["ended_utc"],preclose_state_sha256=before_digest,reopened_state_sha256=after_digest,random_state_scope="observed-state-preservation-only",reopened_process=reopened["process"],reopened_owner_id=reopened["owner_id"])
    c.write_new(directory/"lifecycle.json",lifecycle)
    validate(directory,target,manifest,lifecycle)


def main():
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode",choices=("snapshot","write","verify","assemble","ready"))
    parser.add_argument("directory",type=pathlib.Path)
    parser.add_argument("target",choices=c.TARGETS)
    parser.add_argument("--timeout",type=float,default=180)
    parser.add_argument("--cleanup-timeout",type=float,default=10)
    parser.add_argument("--outer-timeout",type=float)
    parser.add_argument("--addr")
    parser.add_argument("--prefix",choices=("server","reopened","fixture-write","fixture-reopened"),default="server")
    parser.add_argument("--stage",choices=("loaded","preclose","reopened"))
    args=parser.parse_args()
    try:
        with c.outer_deadline(args.outer_timeout if args.outer_timeout is not None else args.timeout+3*args.cleanup_timeout+10):
            if args.mode=="ready": wait_owner(args.directory,args.target,args.prefix,args.timeout)
            elif args.mode=="assemble": assemble(args.directory,args.target)
            else: run_helper(args.directory,args.target,args.addr,args.mode,args.stage,args.timeout,args.cleanup_timeout)
        return 0
    except (ValueError,TypeError,KeyError,OSError) as error:
        if args.mode=="ready" or isinstance(error,TimeoutError):
            stem=args.prefix+".readiness" if args.mode=="ready" else "helper-"+args.mode
            c.write_new(args.directory/(stem+".failure.json"),dict(schema_version=1,kind="owner_readiness_failure" if args.mode=="ready" else "helper_outer_deadline_failure",error=str(error),end_utc=c.now()))
        print("invalid live evidence: "+str(error),file=__import__("sys").stderr)
        return 3


if __name__=="__main__": raise SystemExit(main())
