#!/usr/bin/env python3
"""Interleaved YCSB LOAD through a real standalone GoMap and MongoDB."""
import argparse
from contextlib import ExitStack
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
from urllib.parse import urlsplit
import uuid

import idle_checkpoint_ab as lifecycle
import run as recipe

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import ycsb_compare_live as live
from ycsb_mongodb_reference import checked_command

ACK_CASES = {
    "journaled": ("command_wal_durable", "w=1&journal=true"),
    "w1": ("command_wal_relaxed", "w=1"),
}
PUBLICATIONS = "treedb.cache.cow.publications_total"
WRITE_CONCERN = "ycsb.gateway.write_concern."
BINARIES = ("treedb-mongo-gateway", "go-ycsb", "census", "standalone-checkpoint",
            "mongodb-reference-control")
FAILURE_COUNTS = ("PreMutationRejections", "MalformedRejections", "UnsupportedRejections",
                  "TimeoutRejections", "SyncFailures", "DurabilityUnavailableFailures",
                  "SyncBoundaryFailures", "UncertainOutcomes")


def trial_order(pairs):
    for i in range(pairs):
        targets = ("gomap", "mongodb")
        yield from targets if i % 2 == 0 else reversed(targets)


def acknowledged_uri(uri, ack):
    return (uri.split("?")[0].rstrip("/") + "/ycsb?" + ACK_CASES[ack][1] +
            "&directConnection=true&retryWrites=false")


def cow_evidence(before, after, inserts, ack):
    """The snapshots bracket the client process, before any explicit drain."""
    def delta(key):
        values = before.get(key), after.get(key)
        recipe.require(all(isinstance(v, str) and re.fullmatch(r"[0-9]+", v) for v in values),
                       f"missing or invalid {key}")
        increase = int(values[1]) - int(values[0])
        recipe.require(increase >= 0, f"regressed {key}")
        return increase

    publications = delta(PUBLICATIONS)
    recipe.require(publications > 0, "LOAD made no COW publications")
    expected, other = ("Journal", "Visible") if ack == "journaled" else ("Visible", "Journal")
    success = ("Requests", "LogicalWrites", expected+"Requests", expected+"Acknowledgements")
    zero = (*FAILURE_COUNTS, other+"Requests", other+"Acknowledgements")
    counters = {key: delta(WRITE_CONCERN + key) for key in (*success, *zero)}
    recipe.require(all(counters[key] == inserts for key in success),
                   f"{ack} requests, writes or acknowledgements differ from LOAD count: {counters}")
    recipe.require(all(counters[key] == 0 for key in zero), f"unexpected write-concern outcome: {counters}")
    return dict(publications=publications, counters=counters)


def observe(case, boundary, phase, manifest):
    live.phase_stats_rpc(case, "load", boundary, phase, manifest)
    return live.c.read(case/f"load.phase-stats-{boundary}.response.json")["receipt"]["owner"]


def finish_gomap(case, opened, source, trial, bounds):
    prefix = case/"server.evidence"
    checkpoint = recipe.json_command(
        case, "drain", [recipe.BIN/"standalone-checkpoint", "-owner-evidence", prefix,
                        "-source", source, "-trial", trial, "-load-log", case/"load.out"])
    live.checkpoint_proof(checkpoint, opened, bounds())
    recipe.require(live.c.same(checkpoint, live.c.read(prefix/"owner.checkpoint.json")) and
                   live.c.same(checkpoint["request"], live.c.read(prefix/"owner.checkpoint-request.json")),
                   "live checkpoint reply differs from owner's original receipt")
    request = checkpoint["request"]
    recipe.require(request["source_id"] == source and request["fixture_id"] == trial and
                   request["snapshot_sha256"] == recipe.sha(case/"load.out"), "live checkpoint identity drifted")
    flush = checkpoint["collections_flush_all"]["elapsed_ns"]
    storage = checkpoint["collections_checkpoint_storage"]["elapsed_ns"]
    return dict(endpoint="collections_flush_all_then_checkpoint_storage",
                elapsed_ns=flush + storage, flush_ns=flush, checkpoint_ns=storage)


def finish_mongodb(case, uri, trial, bounds):
    packet_path, close_path = case/"mongodb-drain.json", case/"mongodb-drain.close.json"
    addr = urlsplit(uri).netloc
    recipe.command(case, "drain", [recipe.BIN/"mongodb-reference-control", "-mode", "checkpoint",
                                  "-addr", addr, "-observed", packet_path, "-receipt", close_path,
                                  "-fixture-id", trial])
    packet, closed = live.c.read(packet_path), live.c.read(close_path)
    recipe.require(packet["address"] == addr and packet["fixture_id"] == closed["fixture_id"] == trial and
                   packet["mode"] == closed["mode"] == "checkpoint" and not closed["result_error"] and
                   closed["observed_sha256"] == recipe.sha(packet_path), "MongoDB drain identity drifted")
    live.c.result(closed["disconnect"], "MongoDB drain client Disconnect", bounds=bounds())
    commands = packet["commands"]
    call = commands["checkpoint"]
    checked_command(call, {"fsync": {"$numberInt": "1"}, "lock": False}, addr, bounds())
    versions = [commands[f"{boundary}.buildInfo"]["reply"]["version"] for boundary in ("before", "after")]
    recipe.require(versions[0] == versions[1], "MongoDB version changed during drain")
    return dict(endpoint="mongodb_fsync_lock_false", elapsed_ns=call["result"]["elapsed_ns"],
                mongodb_version=versions[0])


def run_trial(target, directory, count, threads, plan):
    directory.mkdir()
    case = directory/"treedb_mongo" if target == "gomap" else directory/"mongodb"
    case.mkdir()
    data = case/"db"
    trial = directory.name
    ack, profile = plan["ack"], plan["go_map_profile"]
    recipe.SERVICES = []
    rec = dict(target=target, trial=trial, started_utc=recipe.now(), load_average=os.getloadavg())
    try:
        if target == "gomap":
            evidence = case/"server.evidence"
            evidence.mkdir()
            service = recipe.gateway(case, data, profile, "gateway", extra_env={
                "YCSB_SERVER_RECEIPT_DIR": str(evidence), "YCSB_OWNER_PHASE_STATS": "1",
                "YCSB_OWNER_CHECKPOINT": "1",
            })
        else:
            service = recipe.mongodb(case, data, "server")
        recipe.ready(case, service)
        uri = acknowledged_uri(service["uri"], ack)
        if target == "gomap":
            process = live.c.observe(service["process"].pid)
            recipe.require(process["exe_sha256"] == plan["binaries"]["treedb-mongo-gateway"],
                           "running gateway differs from declared binary")
            recipe.save(case/"server.launch.json", dict(process=process))
            phase = f'{plan["campaign_id"]}:treedb_mongo:load:{uuid.uuid4().hex}'
            manifest = dict(campaign_id=plan["campaign_id"], profile=profile, targets=["treedb_mongo"],
                            run_repeats=1, phase_stats=dict(schema_version=1, enabled=True,
                                                           max_observations=64, rpc_timeout_seconds=5),
                            server_builds=dict(treedb_mongo=dict(expected_owner="cow",
                                                                 source_commit=plan["source"])))
            before = observe(case, "before", phase, manifest)
        client_profile = profile if target == "gomap" else f"mongodb_{ack}"
        rec["load"] = recipe.ycsb_phase(case, "load", uri, count, count, client_profile, threads=threads)
        client = live.c.read(case/"load.command.json")
        rec["load_client_seconds"] = client["elapsed_seconds"]
        def bounds():
            return (live.c.timestamp(client["ended_utc"]), live.c.timestamp(recipe.now()))

        if target == "gomap":
            after = observe(case, "after", phase, manifest)
            rec["owner"] = dict(profile=after["profile"], backend=after["backend"],
                                memtable_mode=after["stats"]["treedb.cache.memtable_mode"],
                                **cow_evidence(before["stats"], after["stats"], count, ack))
            rec["drain"] = finish_gomap(case, before, plan["source"], trial, bounds)
        else:
            rec["drain"] = finish_mongodb(case, service["uri"], trial, bounds)
        census = recipe.census(case, "loaded-census", service["uri"], "ycsb", "usertable", count, "ycsb")
        recipe.require(census["success"] and census["observed_count"] == count and
                       all(census[k] == 0 for k in ("missing_count", "extra_count", "duplicate_count")),
                       "LOAD census did not verify the expected fixture")
        rec["census_sha256"] = census["canonical_typed_state_sha256"]
        recipe.stop(case, service)
    except BaseException as error:
        recipe.save_receipt(directory/"failure.json", dict(error=repr(error), trial=rec), error)
        for service in recipe.SERVICES:
            try:
                recipe.stop(case, service, abort=True)
            except BaseException as cleanup_error:
                recipe.report_secondary_failure("LOAD service cleanup", cleanup_error)
        raise
    rec["ended_utc"] = recipe.now()
    recipe.save(directory/"trial.json", rec)
    return rec


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", required=True, type=Path, help="prebuilt binaries from the README recipe")
    parser.add_argument("--out", required=True, type=Path, help="fresh local output directory")
    parser.add_argument("--mongo-image-id", required=True, help="local immutable Docker image ID")
    parser.add_argument("--lane-lock", action="append", type=Path, required=True, help="shared host measurement lock")
    parser.add_argument("--ack", choices=tuple(ACK_CASES), default="journaled")
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--threads", type=int, default=64)
    parser.add_argument("--pairs", type=int, default=3)
    args = parser.parse_args()
    recipe.require(sys.platform == "linux", "standalone owner evidence uses Linux procfs")
    recipe.require(min(args.count, args.threads, args.pairs) > 0, "counts, threads and pairs must be positive")
    recipe.require(re.fullmatch(r"sha256:[0-9a-f]{64}", args.mongo_image_id), "immutable MongoDB image ID required")
    recipe.REPO = Path(__file__).resolve().parents[2]
    recipe.BIN = args.bin_dir.resolve(strict=True)
    recipe.IMAGE = args.mongo_image_id
    out = args.out.resolve()
    locks = [p.resolve() for p in args.lane_lock]
    recipe.require(all(not p.is_relative_to(out) for p in locks), "measurement locks must be outside output")
    lifecycle.require_default_environment(os.environ, recipe.ENV)
    binaries = {name: recipe.sha(recipe.BIN/name) for name in BINARIES}
    recipe.FREEZE = dict(binaries=binaries)
    source = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=recipe.REPO, text=True).strip()
    recipe.require(not subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no"],
                                               cwd=recipe.REPO, text=True), "clean tracked benchmark source required")
    profile, uri_options = ACK_CASES[args.ack]
    with lifecycle.termination_scope(), ExitStack() as stack:
        lifecycle.lock_lane(stack, locks)
        out.mkdir(parents=True, exist_ok=False)
        plan = dict(schema_version=1, campaign_id=uuid.uuid4().hex, source=source, binaries=binaries,
                    host=platform.node(), kernel=platform.release(), cpus=len(os.sched_getaffinity(0)),
                    mongo_image=recipe.IMAGE, count=args.count, threads=args.threads,
                    order=list(trial_order(args.pairs)), ack=args.ack, ack_uri_options=uri_options,
                    go_map_profile=profile, started_utc=recipe.now())
        recipe.save(out/"plan.json", plan)
        results = []
        for i, target in enumerate(plan["order"], 1):
            # No results from an interrupted or invalid trial enter the summary.
            for name, sha256 in binaries.items():
                recipe.require(recipe.sha(recipe.BIN/name) == sha256, f"binary changed: {name}")
            results.append(run_trial(target, out/f"{i:02d}-{target}", args.count, args.threads, plan))
        recipe.save(out/"results.json", dict(plan=plan, trials=results))


if __name__ == "__main__":
    main()
