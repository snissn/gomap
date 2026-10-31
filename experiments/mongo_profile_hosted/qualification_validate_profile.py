#!/usr/bin/env python3
"""Diagnostic profile bindings plus the unchanged maintained lifecycle validator.

This validates receipts and file custody, not pprof syntax, a new producer build,
runtime performance, crash survival, or the public campaign's launch gates.
"""
import argparse
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source"))
import ycsb_compare_validate as c

LIMIT = 64 * 1024 * 1024
NAMES = {"run_1.cpu.pprof", "run_1.block.pprof", "run_1.mutex.pprof"}


def validate(campaign, profiles, mode):
    campaign, profiles = pathlib.Path(campaign), pathlib.Path(profiles)
    c.require(mode in {"cpu_wait", "off"}, "invalid requested profile mode")
    c.require(not profiles.is_symlink() and profiles.is_dir(), "invalid profile directory")
    # Do not replace original validation with matching aggregate strings. This
    # calls the complete pinned consumer, including live.validate and raw owners.
    c.require(c.validate(campaign) == 0, "original maintained lifecycle validation failed")
    manifest = c.read(campaign / "manifest.json")
    target = "treedb_mongo"
    c.require(manifest["targets"] == [target] and manifest["run_repeats"] == 1,
              "diagnostic requires exactly one GoMap gateway target and run")
    directory = campaign / target
    owner = c.read(directory / "server.evidence" / "owner.open.json")
    before_close = c.read(directory / "server.evidence" / "owner.before-close.json")
    run = c.read(directory / "run_1.launch.json")
    loaded = c.read(directory / "snapshot_loaded.launch.json")
    preclose = c.read(directory / "snapshot_preclose.launch.json")
    for name in ("phase.start.json", "phase.stop.json"):
        path = profiles / name
        c.require(not path.is_symlink() and path.is_file(), "missing/unsafe " + name)
    started, stopped = [c.read(profiles / name) for name in ("phase.start.json", "phase.stop.json")]
    for receipt, kind in ((started, "standalone_diagnostic_profile_start"),
                          (stopped, "standalone_diagnostic_profile_stop")):
        c.require(c.schema(receipt) and receipt["kind"] == kind, "invalid profile receipt schema/kind")
        c.require(receipt["phase"] == "run_1" and receipt["mode"] == mode, "profile phase/mode mismatch")
        c.require(c.same(receipt["owner"], owner), "profile original owner binding mismatch")
    begin, ready = c.timestamp(started["begin_utc"]), c.timestamp(started["ready_utc"])
    request, finished = c.timestamp(stopped["stop_requested_utc"]), c.timestamp(stopped["finished_utc"])
    c.require(c.same(stopped["start_utc"], started["ready_utc"]), "profile start receipt mismatch")
    c.require(c.timestamp(owner["observed_utc"]) <= c.timestamp(loaded["end_utc"]) <= begin <= ready
              <= c.timestamp(run["start_utc"]) <= c.timestamp(run["end_utc"]) <= request <= finished
              <= c.timestamp(preclose["start_utc"]) <= c.timestamp(before_close["observed_utc"]),
              "profile phase does not enclose only the selected measured client interval")
    c.result(stopped["finalize"], "profile finalization", bounds=(request, finished))
    c.require(type(stopped["limit_bytes"]) is int and stopped["limit_bytes"] == LIMIT,
              "profile output limit changed")
    artifacts = stopped["artifacts"]
    c.require(isinstance(artifacts, list), "invalid profile artifact list")
    expected = NAMES if mode == "cpu_wait" else set()
    names = [item["name"] for item in artifacts]
    c.require(len(names) == len(set(names)) and set(names) == expected, "missing/duplicate/unexpected profile artifacts")
    c.require({path.name for path in profiles.glob("*.pprof")} == expected,
              "unreceipted profile artifacts")
    total = 0
    for item in artifacts:
        path = profiles / item["name"]
        c.require(not path.is_symlink() and path.is_file(), "missing/unsafe profile artifact")
        size = item["bytes"]
        c.require(type(size) is int and 0 < size <= LIMIT and path.stat().st_size == size,
                  "profile artifact size mismatch")
        c.require(isinstance(item["sha256"], str) and c.SHA.fullmatch(item["sha256"])
                  and c.digest(path) == item["sha256"], "profile artifact hash mismatch")
        total += size
    c.require(type(stopped["bytes_written"]) is int and stopped["bytes_written"] == total <= LIMIT,
              "profile output cap/accounting mismatch")
    return dict(status="profile-bindings-and-maintained-lifecycle-valid", mode=mode,
                artifact_count=len(artifacts), bytes_written=total,
                scope="receipt/file custody only; producer build/runtime and pprof parse remain unqualified")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("campaign", type=pathlib.Path)
    parser.add_argument("profiles", type=pathlib.Path)
    parser.add_argument("--mode", choices=("cpu_wait", "off"), required=True)
    args = parser.parse_args()
    try:
        result = validate(args.campaign, args.profiles, args.mode)
    except (ValueError, KeyError, TypeError, OSError) as error:
        print(json.dumps(dict(status="invalid", error=str(error))))
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
