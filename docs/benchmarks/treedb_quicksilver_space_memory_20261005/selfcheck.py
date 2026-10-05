#!/usr/bin/env python3
"""Bounded hostile-input checks; no native execution or database access."""
import copy
import json
import sys

sys.dont_write_bytecode = True
import extract


def rejected(name, action):
    try:
        action()
    except (ValueError, KeyError):
        return name
    raise RuntimeError("accepted mutation: " + name)


def main():
    contract, raw = extract.load()
    result = extract.extract(contract, raw)
    extract.require(result["publication_status"] == "PROVISIONAL" and result["pending"],
                    "provisional status lost")
    extract.require(result["full_reduction"]["apparent_excluding_wal_bytes"] == 1342108500,
                    "historical storage difference changed")
    extract.require(result["maintenance"][1]["status"] == "timed_out"
                    and result["maintenance"][1]["kernel_process_rss_hwm_bytes"] is None,
                    "timeout invented a completed measurement")
    extract.require(result["failed_batch8192"]["status"] == "failed",
                    "failed copy promoted to successful maintenance")
    contract_bytes = (extract.HERE / "inputs.json").read_bytes()
    # load() returns decompressed aliases; inventory authentication checks stored files.
    stored = {key: raw[key] for key in contract["raw_files"]}
    checks = []
    checks.append(rejected("raw timing bytes changed", lambda: extract.verify_inputs(
        contract_bytes, dict(stored, **{extract.BASE + "commands.json": b"[]"}))))
    missing = dict(stored)
    missing.pop(extract.BASE + "commands.json")
    checks.append(rejected("raw omission", lambda: extract.verify_inputs(contract_bytes, missing)))
    addition = dict(stored, extra=b"unreviewed")
    checks.append(rejected("raw addition", lambda: extract.verify_inputs(contract_bytes, addition)))
    rewritten = copy.deepcopy(contract)
    rewritten["raw_files"].pop(extract.BASE + "commands.json")
    checks.append(rejected("omission from receipt and bytes together", lambda:
                           extract.verify_inputs(extract.encoded(rewritten), missing)))
    rewritten["publication_status"] = "COMPLETE"
    rewritten["pending"] = []
    checks.append(rejected("unauthorized completion contract", lambda:
                           extract.verify_inputs(extract.encoded(rewritten), stored)))

    def altered(name, key, value):
        changed = dict(raw)
        record = json.loads(changed[name])
        record[key] = value
        changed[name] = extract.encoded(record)
        return changed

    checks.append(rejected("oracle count changed", lambda: extract.extract(contract,
        altered(extract.BASE + "full-verify-stdout.json", "verified_keys", 2999999))))
    checks.append(rejected("completion flags changed", lambda: extract.extract(contract,
        altered(extract.BASE + "full-stdout.json", "byte_minimized", True))))
    checks.append(rejected("unknown landed source", lambda: extract.extract(contract,
        altered("baseline-landed-source-equality.json", "landed_merge", "0" * 40))))
    census = json.loads(raw[extract.BASE + "before-maintenance-census.json"])
    census["apparent_excluding_wal"] += 1
    checks.append(rejected("wrong WAL accounting", lambda: extract.census(census)))
    commands = json.loads(raw[extract.BASE + "commands.json"])
    commands[3]["timed_out"] = False
    commands[3]["rc"] = 0
    changed = dict(raw)
    changed[extract.BASE + "commands.json"] = extract.encoded(commands)
    checks.append(rejected("timeout promoted to success", lambda: extract.extract(contract, changed)))
    checks.append(rejected("reconstruction promoted to original build receipt", lambda:
        extract.extract(contract, altered("maintenance/baseline-treemap-reconstruction.json",
                                         "original_build_receipt_missing", False))))
    checks.append(rejected("leaf diagnostic producer manifest changed", lambda:
        extract.extract(contract, altered("leaf256/run.json", "manifest_sha256", "0" * 64))))
    checks.append(rejected("leaf diagnostic oracle count changed", lambda:
        extract.extract(contract, dict(raw, **{"leaf256/stdout.json": extract.encoded([
            dict(json.loads(raw["leaf256/stdout.json"])[0], verified_keys=2999999)])}))))
    checks.append(rejected("imported matrix plan changed", lambda:
        extract.extract(contract, altered("pending-plans/offsets-3m-matched-plan.json", "repeats", 2))))
    pending_plan = json.loads(raw["pending-plans/offsets-3m-structural-diagnostic-plan.json"])
    pending_plan["cells"][0]["profiled"] = False
    checks.append(rejected("structural diagnostic class changed", lambda:
        extract.extract(contract, dict(raw, **{"pending-plans/offsets-3m-structural-diagnostic-plan.json":
                                               extract.encoded(pending_plan)}))))
    public = result["public_matched"]
    extract.require(public["status"] == "VALIDATED_ALL" and public["observed_rows"] == 18
                    and public["acceptance"] == "HELD" and len(public["comparisons"]) == 12,
                    "public inventory promoted to acceptance or incomplete")
    path = "public-matched/offsets-3m-matched/1-candidate-durable-primary/"
    for label, field, replacement in [
        ("public binary mismatch", "source", dict(public["sources"]["candidate"], binary_sha256="0" * 64)),
        ("public command missing arguments", "command", public["records"][1]["command"][:-2]),
        ("public plan binding mismatch", "plan_sha256", "0" * 64),
        ("public collector mismatch", "collector_sha256", "0" * 64),
        ("public GC environment changed", "env", dict(public["environment"], GOGC="off")),
        ("public loader provenance missing", "native_resolution", {}),
        ("public unvalidated row", "validated", False),
        ("public miss screening omitted", "miss_ratio_validation", [])]:
        checks.append(rejected(label, lambda field=field, replacement=replacement:
            extract.public_matched(contract, altered(path + "run.json", field, replacement))))
    for label, mutate in [
        ("public oracle incomplete", lambda v:v.update(verified_keys=2999999)),
        ("public throughput ratio changed", lambda v:v["phases"][0].update(ops_per_sec=1)),
        ("public requested miss count changed", lambda v:v["phases"][2].update(requested_absent=0)),
        ("public missing checkpoint guardrail", lambda v:v.update(checkpoint_ms=[]))]:
        values = json.loads(raw[path + "stdout.json"])
        mutate(values[0])
        changed = dict(raw, **{path + "stdout.json": extract.encoded(values)})
        checks.append(rejected(label, lambda changed=changed: extract.public_matched(contract, changed)))
    missing = dict(raw)
    missing.pop(path + "run.json")
    checks.append(rejected("public row missing", lambda: extract.public_matched(contract, missing)))
    checks.append(rejected("public native loader output changed", lambda: extract.public_matched(
        contract, dict(raw, **{path + "ldd.stdout.txt": b"unresolved"}))))
    observer_path = "public-matched/offsets-3m-matched-memory.jsonl"
    changed = dict(raw)
    observations = raw[observer_path].splitlines()
    row = json.loads(observations[0])
    row["exe"] = "/unowned-process"
    changed[observer_path] = b"\n".join([extract.encoded(row).strip()] + observations[1:]) + b"\n"
    checks.append(rejected("public RSS observation unbound", lambda: extract.public_matched(contract, changed)))
    profiles = result["public_profiles"]
    extract.require(profiles["status"] == "VALIDATED_ALL" and profiles["acceptance"] == "EVIDENCE_ONLY"
                    and profiles["observed_rows"] == 6 and len(profiles["native_profile_inventory"]) == 78
                    and len(profiles["top_analyses"]["rows"]) == 24, "profile inventory/qualification differs")
    path = "public-profiles/offsets-3m-profile-paired/1-candidate-durable-primary-profile/"
    cell = dict(profiles["records"][1]["cell"], profiled=False)
    checks.append(rejected("profile capture disabled", lambda: extract.public_matched(contract,
        altered(path + "run.json", "cell", cell), profiled=True)))
    checks.append(rejected("profile output directory omitted", lambda: extract.public_matched(contract,
        altered(path + "run.json", "command", profiles["records"][1]["command"][:-2]), profiled=True)))
    for label, mutate in [
        ("profile GC heap bracket negative", lambda v:v["phases"][0].update(process_heap_alloc_before=-1)),
        ("profile cache snapshot omitted", lambda v:v["phases"][0].update(stats_before={})),
        ("profile oracle incomplete", lambda v:v.update(verified_misses=0))]:
        values = json.loads(raw[path + "stdout.json"])
        mutate(values[0])
        changed = dict(raw, **{path + "stdout.json":extract.encoded(values)})
        checks.append(rejected(label, lambda changed=changed:extract.public_matched(contract, changed, profiled=True)))
    ledger_path = "public-profiles/m-profile-analysis-v2/inventory.json"
    inventory = json.loads(raw[ledger_path])
    inventory.pop()
    checks.append(rejected("profile trace ledger omission", lambda:extract.public_matched(contract,
        dict(raw, **{ledger_path:extract.encoded(inventory)}), profiled=True)))
    analysis_path = "public-profiles/m-profile-analysis-v2/analyses.json"
    for label, field, replacement in [("profile analysis tool changed", "go_sha256", "0" * 64),
                                      ("profile analysis missing", "rows", profiles["top_analyses"]["rows"][:-1])]:
        checks.append(rejected(label, lambda field=field, replacement=replacement:extract.public_matched(contract,
            altered(analysis_path, field, replacement), profiled=True)))
    analyses = json.loads(raw[analysis_path])
    analyses["rows"][0]["profile_sha256"] = "0" * 64
    checks.append(rejected("profile analysis digest changed", lambda:extract.public_matched(contract,
        dict(raw, **{analysis_path:extract.encoded(analyses)}), profiled=True)))
    checks.append(rejected("profile producer receipt differs from public", lambda:extract.public_matched(contract,
        altered("public-profiles/qualified-manifest-offsets.json", "provisional", False), profiled=True)))
    structural = result["structural"]
    extract.require(structural["status"] == "VALIDATED_ALL"
                    and structural["acceptance"] == "STRUCTURAL_EVIDENCE_ONLY"
                    and structural["per_cache_record_count"] == 864
                    and len(structural["qualified_stats_before"]) == 8
                    and len(structural["sampled_full_heap"]) == 16,
                    "structural scope lost")
    cache_path = "structural/offsets-3m-diagnostic-cache.jsonl"
    cache_rows = [json.loads(line) for line in raw[cache_path].splitlines()]
    first_capture = structural["qualified_stats_before"][0]
    first = next(i for i,v in enumerate(cache_rows)
                 if v["PID"] == first_capture["pid"] and v["UnixNano"] == first_capture["capture_start_ns"])
    for label, mutate in [
        ("structural live histogram changed", lambda v:v[first]["LiveK"].update({"2":0})),
        ("structural reflected slot size changed", lambda v:v[first].update(SlotSizeBytes=136)),
        ("structural retained empty capacity omitted", lambda v:v[first].update(EmptySlotOffsetCapacity={})),
        ("structural byte total changed", lambda v:v[first].update(StructuralMetadataBytes=0)),
        ("structural process identity changed", lambda v:v[first].update(PID=0)),
        ("structural aggregate hit count changed", lambda v:v[first]["Stats"].update(Hits=v[first]["Stats"]["Hits"]+1)),
        ("structural shared budget summed", lambda v:v[first]["Stats"].update(BudgetBytes=v[first]["Stats"]["BudgetBytes"]*2)),
        ("structural duplicate cache in capture", lambda v:v[first+1].update(Cache=v[first]["Cache"])),
        ("structural capture gap changed", lambda v:v[first+1].update(UnixNano=v[first]["UnixNano"]+10000001))]:
        values = copy.deepcopy(cache_rows)
        mutate(values)
        changed = dict(raw, **{cache_path:("\n".join(json.dumps(v, sort_keys=True) for v in values)+"\n").encode()})
        checks.append(rejected(label, lambda changed=changed:extract.structural_evidence(structural["records"], contract, changed)))
    snapshot_path = "structural/m-diagnostic-analysis/full-snapshot-index.json"
    for label, mutate in [
        ("structural snapshot digest changed", lambda v:v[0].update(sha256="0"*64)),
        ("structural snapshot PID changed", lambda v:v[0].update(pid=0)),
        ("structural snapshot phase order changed", lambda v:v.reverse())]:
        values = json.loads(raw[snapshot_path])
        mutate(values)
        changed = dict(raw, **{snapshot_path:extract.encoded(values)})
        checks.append(rejected(label, lambda changed=changed:extract.structural_evidence(structural["records"], contract, changed)))
    checks.append(rejected("structural analysis tool changed", lambda:extract.structural_evidence(
        structural["records"], contract, altered("structural/m-diagnostic-analysis/analyses.json", "go_sha256", "0"*64))))
    builds = json.loads(raw["structural/5004-diagnostic-overlay/native-builds.json"])
    builds["baseline"]["binary_sha256"] = "0"*64
    checks.append(rejected("structural overlay binary changed", lambda:extract.structural_evidence(structural["records"], contract,
        dict(raw, **{"structural/5004-diagnostic-overlay/native-builds.json":extract.encoded(builds)}))))
    records = copy.deepcopy(structural["records"])
    records[0]["phases"][0]["reported_cache_snapshots"]["stats_before"]["treedb.vlog.grouped_frame_cache.hits"] = "0"
    checks.append(rejected("structural phase aggregate changed", lambda:extract.structural_evidence(records, contract, raw)))
    restored = result["restored_maintenance"]
    extract.require(len(restored["attempts"]) == 3 and restored["status"] == "MEASURED_PROVISIONAL"
                    and restored["input_metadata"].startswith("inherited baseline"), "restored input boundary lost")
    r2 = "restore-r2/"
    base = r2 + "maintenance-restore-r2-3m-batch8192/"
    for label, path, field, replacement in [
        ("restored source head changed", "receipts-restore-r2/source.json", "head", "0" * 40),
        ("restored landed tree changed", "restore-r2-landed-source-equality.json", "landed_tree", "0" * 40),
        ("restored causal red promoted", "restore-r2-causal/red-receipt.json", "rc", 0),
        ("restored causal green failed", "restore-r2-causal/green-receipt.json", "rc", 1),
        ("restored inherited profile time changed", "restore-r2-batch-3m-8192-input/run.json", "started", 0),
        ("restored readonly mutation", "restore-r2-batch-3m-8192-input/readonly-after-restore/receipt.json", "unchanged", False),
        ("restored completion promoted", "maintenance-restore-r2-3m-batch8192/exhaustive-stdout.json", "byte_minimized", True),
        ("restored mode changed", "maintenance-restore-r2-3m-batch8192/full-stdout.json", "mode", "exhaustive"),
        ("restored oracle incomplete", "maintenance-restore-r2-3m-batch8192/exhaustive-second-verify-stdout.json", "verified_keys", 2999999)]:
        checks.append(rejected(label, lambda path=path, field=field, replacement=replacement:
            extract.restored_maintenance(contract, altered(r2 + path, field, replacement))))
    checks.append(rejected("restored helper source changed", lambda: extract.restored_maintenance(contract,
        dict(raw, **{r2 + "maintenance-diagnostic-overlay/rebind-copy.go.txt": b"replacement"}))))
    checks.append(rejected("restored template/layout coverage removed", lambda: extract.restored_maintenance(contract,
        dict(raw, **{r2 + "restore-r2-causal/green-stdout.txt": raw[r2 + "restore-r2-causal/green-stdout.txt"].replace(
            b"PASS: TestRebindDurableRootSnapshotSideStoreNamespaceMatchesFreshAuthority/template/directory-v2", b"omitted")}))))
    commands = json.loads(raw[base + "commands.json"])
    commands[3]["command"][-1] = "256"
    checks.append(rejected("restored rewrite batch changed", lambda: extract.restored_maintenance(contract,
        dict(raw, **{base + "commands.json": extract.encoded(commands)}))))
    checks.append(rejected("restored pre-oracle census boundary changed", lambda: extract.restored_maintenance(contract,
        altered(base + "exhaustive-before-oracle-census.json", "apparent_excluding_wal", 0))))
    churn_prefix = "churn/pair/"
    churn_path = contract["churn"]["available_stdout"]["defaults"]
    for label, mutate in [
        ("churn round omission", lambda v:v[0]["maintenance_churn"]["rounds"].pop()),
        ("churn full refresh missing", lambda v:v[0]["maintenance_churn"]["rounds"][0].update(restored_keys=40000)),
        ("churn miss oracle missing", lambda v:v[0]["maintenance_churn"]["rounds"][0].update(verified_misses=6030000)),
        ("churn workload setting changed", lambda v:v[0]["maintenance_churn"].update(leaf_generation_pack_maintenance_env="1")),
        ("churn time bracket changed", lambda v:v[0]["maintenance_churn"]["rounds"][0].update(wall_seconds=1)),
        ("churn missing RSS snapshot", lambda v:v[0]["maintenance_churn"]["rounds"][0]["after"].update(process_rss_supported=False)),
        ("churn invalid closed file path", lambda v:v[0]["maintenance_churn"]["final_files_after_close"].update({"../escape":1}))]:
        values = json.loads(raw[churn_path])
        mutate(values)
        checks.append(rejected(label, lambda values=values:extract.churn_observations(contract,
            dict(raw, **{churn_path:extract.encoded(values)}))))
    for label, path, key, value in [
        ("churn source identity changed", "receipts-churn-v3/source.json", "head", "0"*40),
        ("churn source inventory changed", "receipts-churn-v3/source.json", "compiled_project_inputs", {}),
        ("churn failed run promoted", "churn-3m-1-defaults/1-treedb-churn-defaults/run.json", "rc", 1),
        ("churn command changed", "churn-3m-1-defaults/1-treedb-churn-defaults/run.json", "command", []),
        ("churn loader proof changed", "churn-3m-1-defaults/1-treedb-churn-defaults/run.json", "native_resolution", {})]:
        checks.append(rejected(label, lambda path=path,key=key,value=value:extract.churn_observations(contract,
            altered(churn_prefix+path,key,value))))
    checks.append(rejected("churn actual collector replaced", lambda:extract.churn_observations(contract,
        dict(raw, **{churn_prefix+"measurement-capture.py":b"replacement"}))))
    samples_path = churn_prefix + "churn-3m-1-defaults-memory.jsonl"
    samples = [json.loads(line) for line in raw[samples_path].splitlines()]
    samples[0]["time"] = 0
    checks.append(rejected("churn observer outside run", lambda:extract.churn_observations(contract,
        dict(raw, **{samples_path:b"".join(json.dumps(row).encode()+b"\n" for row in samples)}))))
    failed_prefix = "churn/full-failures/maintenance-churn-3m-defaults-full8192/"
    for label,path,key,value in [
        ("churn readonly mutation", "readonly-after-failure/receipt.json", "unchanged", False),
        ("churn readonly incomplete", "readonly-after-failure/stdout.json", "verified_keys", 2999999)]:
        checks.append(rejected(label, lambda path=path,key=key,value=value:extract.churn_full_failures(contract,
            altered(failed_prefix+path,key,value),result["churn"])))
    commands = json.loads(raw[failed_prefix+"commands.json"])
    commands[1]["rc"] = 0
    checks.append(rejected("churn Full failure promoted", lambda:extract.churn_full_failures(contract,
        dict(raw, **{failed_prefix+"commands.json":extract.encoded(commands)}),result["churn"])))
    checks.append(rejected("churn Full completed output invented", lambda:extract.churn_full_failures(contract,
        dict(raw, **{failed_prefix+"full-stdout.json":b"{}"}),result["churn"])))
    checks.append(rejected("scoped source relabeled whole tree", lambda:extract.current_main_applicability(contract,
        altered("applicability/current-main-runtime-applicability.json","historical_whole_tree_receipts_unchanged",False))))
    replay = "replay-failures/"
    for label, path, key, value in [
        ("replay failed guard promoted", "replay-3m/1-baseline/run.json", "rc", 0),
        ("replay child failure mislabeled", "replay-3m/1-baseline/guard/run.json", "rc", 1),
        ("replay guard source changed", "5004-replay-diagnostic-v2/source-receipt.json", "candidate", "0"*40),
        ("replay guard exception expanded", "replay-3m/seal.json", "format_identity_exceptions", ["maindb/index.db"]),
        ("replay backup omitted", "replay-3m/seal.json", "backup", {}),
        ("replay original preservation lost", "replay-3m-no-vacuum/original-preserved.json", "current_matches_failed_census", False),
        ("replay native source changed", "replay-3m/1-baseline/run.json", "binary_sha256", "0"*64)]:
        checks.append(rejected(label, lambda path=path,key=key,value=value:extract.replay_failures(contract,
            altered(replay+path,key,value))))
    for label,path,mutate in [
        ("replay rejected inode unchanged", "replay-3m/1-baseline/guard/post/census.json", lambda v:v["maindb/index.db"].update(
            ino=json.loads(raw[replay+"replay-3m/seal.json"])["original"]["maindb/index.db"]["ino"])),
        ("vacuum-off knob missing", "replay-3m-no-vacuum/1-baseline/run.json", lambda v:v["env"].pop("TREEDB_DISABLE_BACKGROUND_INDEX_VACUUM")),
        ("replay native resource omitted", "replay-native-retained-blobs.json", lambda v:v.pop()),
        ("replay initial oracle incomplete", "replay-3m/1-baseline/stdout.json", lambda v:v[0].update(initial_verified_keys=2999999)),
        ("replay read phase missing", "replay-3m/1-baseline/stdout.json", lambda v:v[0]["phases"].pop()),
        ("replay create phases invented", "replay-3m/create/stdout.json", lambda v:v[0].update(phases=[]))]:
        value = json.loads(raw[replay+path])
        mutate(value)
        checks.append(rejected(label, lambda path=path,value=value:extract.replay_failures(contract,
            dict(raw, **{replay+path:extract.encoded(value)}))))
    extract.require(result["replay_failures"]["accepted_reads"] == 0
                    and result["replay_failures"]["acceptance"] == "HELD", "replay failures clear public gate")
    repair = "manifest-l/"
    for label,path,key,value in [
        ("manifest repair source relabeled", "receipts-manifest-l/source.json", "head", "0"*40),
        ("manifest repair compiled inventory omitted", "receipts-manifest-l/source.json", "compiled_project_inputs", {}),
        ("manifest repair failed original modified", "churn-3m-defaults-manifest-l-input/original-preserved.json", "unchanged", False),
        ("manifest repair terminal binary changed", "maintenance-churn-3m-defaults-manifest-l-full8192/identity.json", "treemap_sha256", "0"*64),
        ("manifest repair oracle incomplete", "maintenance-churn-3m-defaults-manifest-l-full8192/full-verify-stdout.json", "verified_misses", 6030000),
        ("manifest repair completion promoted", "maintenance-churn-3m-defaults-manifest-l-full8192/full-stdout.json", "byte_minimized", True),
        ("manifest repair landed tree changed", "manifest-l-landed-source-equality.json", "landed_tree", "0"*40),
        ("manifest repair unqualified manifest", "qualified-manifest-manifest-l.json", "provisional", True),
        ("manifest repair postcampaign inputs changed", "final/manifest-l-post-campaign-receipt.json", "all_frozen_inputs_unchanged", False),
        ("single Exhaustive retries allowed", "final/maintenance-churn-manifest-l-exhaustive-method.json", "no_retries", False),
        ("single Exhaustive oracle incomplete", "final/maintenance-churn-3m-defaults-manifest-l-exhaustive8192/exhaustive-verify-stdout.json", "verified_keys", 2999999)]:
        checks.append(rejected(label, lambda path=path,key=key,value=value:extract.manifest_l_maintenance(contract,
            altered(repair+path,key,value))))
    for label,path,mutate in [
        ("manifest repair causal red promoted", "manifest-l-native-tests/commands.json", lambda v:v[0].update(rc=0)),
        ("manifest repair race failed", "manifest-l-native-tests/commands.json", lambda v:v[2].update(rc=1)),
        ("manifest repair inherited timings changed", "churn-3m-defaults-manifest-l-input/run.json", lambda v:v.update(started=0)),
        ("manifest repair Full failed", "maintenance-churn-3m-defaults-manifest-l-full8192/commands.json", lambda v:v[1].update(rc=1)),
        ("manifest repair batch changed", "maintenance-churn-3m-defaults-manifest-l-full8192/commands.json", lambda v:v[1]["command"].__setitem__(-1,"128")),
        ("single Exhaustive original preservation lost", "final/manifest-l-post-campaign-receipt.json", lambda v:v["originals"][0].update(original_unchanged_after_fixed_maintenance=False))]:
        value=json.loads(raw[repair+path]);mutate(value)
        checks.append(rejected(label,lambda path=path,value=value:extract.manifest_l_maintenance(contract,
            dict(raw, **{repair+path:extract.encoded(value)}))))
    prose = (extract.HERE / "REPORT.md").read_text()
    checks.append(rejected("publication prose changed", lambda:
        extract.report(result, prose.replace("The measured candidate is held from merge; final publication remains pending.",
                                             "The measured candidate is accepted for merge; final publication is complete."))))
    extract.require((extract.HERE / "RESULTS.json").read_bytes() == extract.encoded(result),
                    "RESULTS differs from extraction")
    extract.require((extract.HERE / "REPORT.md").read_text() == extract.report(result),
                    "REPORT differs from extraction")
    print(json.dumps({"status": "PASS", "optimized": not __debug__,
                      "mutation_checks": checks}, sort_keys=True))


if __name__ == "__main__":
    main()
