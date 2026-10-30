#!/usr/bin/env python3
"""Required live-owner and typed-snapshot contract for the YCSB harness.

These checks consume original observations and helper call receipts. A matching
pair of supplied digest strings or an activation label is never sufficient.
"""
import base64
import hashlib
import json
import pathlib
import ycsb_compare_validate as c
import ycsb_correctness_validate as exact


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
    c.require(value["profile"] == manifest["profile"] and value["format"] == manifest["formats"][target] and value["target"] == ("native" if target == "treedb_native" else "mongo"), "random snapshot settings mismatch")
    c.require(value["stage"] == "random_"+stage and value["operations"] == [], "invalid random snapshot stage")
    rows = exact.rows(value["rows"], value["format"])
    c.require(set(rows) == expected_keys(manifest["recordcount"]), "random snapshot key set mismatch")
    for fields in rows.values():
        c.require(set(fields) == {"field"+str(i) for i in range(10)}, "random snapshot field set mismatch")
        for typed in fields.values():
            raw = typed["value"] if value["format"] == "json" else typed["base64"]
            c.require(len(base64.b64decode(raw, validate=True)) == 100, "random snapshot field length mismatch")
    canonical = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()
    return rows, hashlib.sha256(canonical).hexdigest()


def owner(directory, prefix, build, profile):
    launch = c.read(directory/(prefix+".launch.json"))
    stopped = c.read(directory/(prefix+".exit.json"))
    c.process(launch["process"])
    c.require(launch["process"]["exe_sha256"] == build["exe_sha256"], "live owner ELF mismatch")
    c.require(stopped["process"] == launch["process"] and stopped["exit_code"] == 0, "live owner checked exit missing")
    evidence = directory/(prefix+".evidence")
    opened, before, closed = [c.read(evidence/name) for name in ("owner.open.json", "owner.before-close.json", "owner.close.json")]
    for observation in (opened,before):
        c.require(c.schema(observation) and observation["kind"] == "live_owner_observation", "missing raw live owner observation")
        c.require(observation["process"] == launch["process"] and observation["data_dir"] == launch["data_dir"], "raw owner process/data mismatch")
        c.require(pathlib.Path(observation["data_dir"]).is_absolute() and type(observation["data_device"]) is int and type(observation["data_inode"]) is int and observation["data_inode"] > 0, "missing data directory inode")
        c.require(isinstance(observation["owner_id"],str) and len(observation["owner_id"]) == 32, "missing actual owner token")
        c.require(type(observation["observed_gomaxprocs"]) is int and observation["observed_gomaxprocs"] > 0, "missing observed runtime GOMAXPROCS")
        c.require(observation["profile"] == profile and observation["stats"]["treedb.profile.resolved"] == profile, "actual owner profile mismatch")
        c.require(observation["backend"] == build["expected_owner"], "actual raw backend mismatch")
        if build["expected_owner"] == "cow":
            c.require(observation["capture_error"] == "" and observation["stats"]["treedb.cache.memtable_mode"] == "cow_btree", "missing actual COW capture/cache evidence")
            seq = observation["stats"]["treedb.cache.cow.txn.logical_seq"]
            c.require(isinstance(seq,str) and seq.isdecimal(), "missing actual COW sequence")
        else:
            c.require(build["expected_owner"] == "legacy" and observation["capture_error"] == "treedb: command kind has no native semantic preparation", "missing actual legacy observation")
        c.timestamp(observation["observed_utc"])
    for key in ("owner_id","process","data_dir","data_device","data_inode","profile","backend"):
        c.require(before[key] == opened[key], "live owner identity changed")
    c.require(c.schema(closed) and closed["kind"] == "original_server_owner_close" and closed["owner_id"] == opened["owner_id"] and closed["process"] == launch["process"], "original owner Close binding mismatch")
    bounds = c.result(closed["close"], "original owner Close")
    c.require(c.timestamp(opened["observed_utc"]) <= c.timestamp(before["observed_utc"]) <= bounds[0], "raw owner ordering mismatch")
    return opened,closed


def helper(directory, stem, manifest, target, mode, fixture_id):
    launch,receipt = [c.read(directory/(stem+suffix)) for suffix in (".launch.json",".close.json")]
    c.process(launch["process"])
    c.require(launch["campaign_id"] == manifest["campaign_id"] and launch["exit_code"] == 0, "snapshot/fixture child failure or stale campaign")
    c.require(launch["process"]["exe_sha256"] == manifest["fixture"]["exe_sha256"], "snapshot/fixture ELF mismatch")
    c.require(launch["receipt_sha256"] == c.digest(directory/(stem+".close.json")), "snapshot/fixture receipt hash mismatch")
    c.require(c.schema(receipt) and receipt["kind"] == ("random_snapshot_client_close" if mode == "snapshot" else "deterministic_fixture_client_close"), "wrong snapshot/fixture receipt kind")
    c.require(receipt["source_id"] == manifest["server_builds"][target]["source_commit"] and receipt["fixture_id"] == fixture_id and receipt["mode"] == mode and receipt["pid"] == launch["process"]["pid"], "snapshot/fixture process/source mismatch")
    c.require(receipt["result_error"] == "" and receipt["close_error"] == "", "snapshot/fixture original client close failed")
    bounds = c.timestamp(launch["start_utc"]),c.timestamp(launch["end_utc"])
    c.require(bounds[0] <= c.timestamp(receipt["started_utc"]) <= c.timestamp(receipt["ended_utc"]) <= bounds[1], "snapshot/fixture call interval mismatch")
    for name,hash_value in receipt["artifacts_sha256"].items():
        c.require(pathlib.Path(name).name == name and hash_value == c.digest(directory/name), "snapshot/fixture artifact hash mismatch")
    required = {stem+".json"} if mode == "snapshot" else {"fixture.expected.json", "fixture.updated.json", "fixture.updated.json.load.json"} if mode == "write" else {"fixture.expected.json", "fixture.reopened.json"}
    c.require(required <= receipt["artifacts_sha256"].keys(), "missing snapshot/fixture artifact binding")
    return receipt,bounds


def validate(directory, target, manifest, lifecycle):
    c.require(manifest.get("lifecycle_contract") == 2, "required live lifecycle contract 2 missing")
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
    c.require(runtime["observed_gomaxprocs"] == original["observed_gomaxprocs"], "aggregate runtime differs from actual observation")
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
        opening = reopened if stage == "reopened" else original
        closing = reopened_close if stage == "reopened" else closed
        c.require(c.timestamp(opening["observed_utc"]) <= bounds[0] <= bounds[1] <= c.timestamp(closing["close"]["start_utc"]), "snapshot not in observed owner lifetime")
    load_launch=c.read(directory/"load.launch.json")
    runs=[c.read(directory/("run_"+str(i)+".launch.json")) for i in range(1,manifest["run_repeats"]+1)]
    c.require(c.timestamp(load_launch["end_utc"]) <= receipts["loaded"][1][0] <= receipts["loaded"][1][1] <= c.timestamp(runs[0]["start_utc"]), "loaded snapshot is not between load and measured runs")
    c.require(max(c.timestamp(value["end_utc"]) for value in runs) <= receipts["preclose"][1][0], "preclose snapshot precedes settled measured clients")
    c.require(snapshots["preclose"][0] == snapshots["reopened"][0], "canonical typed random state preservation mismatch")
    c.require(lifecycle["preclose_state_sha256"] == snapshots["preclose"][1] and lifecycle["reopened_state_sha256"] == snapshots["reopened"][1], "canonical random snapshot digest mismatch")
    checkpoint = receipts["preclose"][0]
    c.require(checkpoint["checkpoint_attempted"] is True and checkpoint["checkpoint_succeeded"] is True, "missing real explicit checkpoint call")
    c.result(checkpoint["checkpoint"],"actual explicit checkpoint",bounds=receipts["preclose"][1])
    c.require(lifecycle["checkpoint"] == checkpoint["checkpoint"] and lifecycle["server_close"] == closed["close"], "aggregate lifecycle differs from actual calls")
    c.require(lifecycle["owner_id"] == original["owner_id"] and lifecycle["reopened_process"] == reopened["process"] and lifecycle["reopened_owner_id"] == reopened["owner_id"], "aggregate reopened owner mismatch")
    # Independently named deterministic fixture uses a fresh database and two
    # independently observed server owners; it cannot stand in for random state.
    first,first_close = owner(directory,"fixture-write",build,manifest["profile"])
    second,second_close = owner(directory,"fixture-reopened",build,manifest["profile"])
    c.require(first["data_dir"] != original["data_dir"] and first["owner_id"] != second["owner_id"] and first["process"] != second["process"], "fixture requires fresh data and independent reopen")
    c.require(all(first[k] == second[k] for k in ("data_dir","data_device","data_inode")), "fixture reopened data mismatch")
    c.require(c.timestamp(first_close["close"]["end_utc"]) <= c.timestamp(second["observed_utc"]), "fixture reopen precedes original Close")
    fixture_id = manifest["campaign_id"]+":"+target+":deterministic"
    for stem,mode,opened,closing in (("fixture_write","write",first,first_close),("fixture_verify","verify",second,second_close)):
        receipt,bounds=helper(directory,stem,manifest,target,mode,fixture_id)
        c.require(c.timestamp(opened["observed_utc"]) <= bounds[0] <= bounds[1] <= c.timestamp(closing["close"]["start_utc"]), "fixture call outside observed owner lifetime")
        if target == "treedb_native" and mode == "write":
            c.require(receipt["checkpoint_attempted"] is True and receipt["checkpoint_succeeded"] is True,"fixture checkpoint failed/skipped")
    expected = c.read(directory/"fixture.expected.json")
    c.require(expected["source_id"] == build["source_commit"] and expected["fixture_id"] == fixture_id and expected["format"] == manifest["formats"][target] and expected["profile"] == manifest["profile"], "deterministic fixture identity mismatch")
    exact.validate(expected,[c.read(directory/name) for name in ("fixture.updated.json.load.json","fixture.updated.json","fixture.reopened.json")])


def run_helper(directory,target,addr,mode,stage=None):
    import argparse
    manifest=c.read(directory.parent/"manifest.json")
    stem="snapshot_"+stage if mode=="snapshot" else "fixture_"+("write" if mode=="write" else "verify")
    identity=manifest["campaign_id"]+":"+target+(":random" if mode=="snapshot" else ":deterministic")
    observed=directory/(stem+".json") if mode=="snapshot" else directory/("fixture.updated.json" if mode=="write" else "fixture.reopened.json")
    command=[manifest["fixture"]["binary"],"-mode",mode,"-target","native" if target=="treedb_native" else "mongo","-addr",addr,"-format",manifest["formats"][target],"-profile",manifest["profile"],"-source-id",manifest["server_builds"][target]["source_commit"],"-fixture-id",identity,"-expected",str(directory/"fixture.expected.json"),"-observed",str(observed),"-receipt",str(directory/(stem+".close.json"))]
    if mode=="snapshot":
        command += ["-stage",stage]
        if stage=="preclose": command += ["-checkpoint"]
    code=c.launch(argparse.Namespace(directory=str(directory),stem=stem,command=command))
    c.require(code==0,"snapshot/fixture child failed with exit "+str(code))


def interval(start,end):
    return dict(attempted=True,success=True,error="",start_utc=start,end_utc=end,elapsed_ns=round((c.timestamp(end)-c.timestamp(start))*1e9))


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
    c.require(before==after,"canonical typed random state preservation mismatch")
    ended=c.now()
    lifecycle=dict(**shared,server_exit_code=0,checkpoint=before_receipt["checkpoint"],server_close=closed["close"],reopen=interval(closed["close"]["end_utc"],reopened["observed_utc"]),random_state_preservation=interval(started,ended),preclose_state_utc=before_receipt["ended_utc"],reopened_state_utc=after_receipt["ended_utc"],preclose_state_sha256=before_digest,reopened_state_sha256=after_digest,random_state_scope="observed-state-preservation-only",reopened_process=reopened["process"],reopened_owner_id=reopened["owner_id"])
    c.write_new(directory/"lifecycle.json",lifecycle)
    validate(directory,target,manifest,lifecycle)


def main():
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode",choices=("snapshot","write","verify","assemble"))
    parser.add_argument("directory",type=pathlib.Path)
    parser.add_argument("target",choices=c.TARGETS)
    parser.add_argument("--addr")
    parser.add_argument("--stage",choices=("loaded","preclose","reopened"))
    args=parser.parse_args()
    try:
        if args.mode=="assemble": assemble(args.directory,args.target)
        else: run_helper(args.directory,args.target,args.addr,args.mode,args.stage)
        return 0
    except (ValueError,TypeError,KeyError,OSError) as error:
        print("invalid live evidence: "+str(error),file=__import__("sys").stderr)
        return 3


if __name__=="__main__": raise SystemExit(main())
