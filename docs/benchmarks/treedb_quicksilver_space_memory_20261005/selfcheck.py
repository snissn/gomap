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
                    and public["acceptance"] == "PENDING" and len(public["comparisons"]) == 12,
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
    prose = (extract.HERE / "REPORT.md").read_text()
    checks.append(rejected("publication prose changed", lambda:
        extract.report(result, prose.replace("Candidate acceptance and final publication remain pending.",
                                             "Candidate acceptance and final publication are complete."))))
    extract.require((extract.HERE / "RESULTS.json").read_bytes() == extract.encoded(result),
                    "RESULTS differs from extraction")
    extract.require((extract.HERE / "REPORT.md").read_text() == extract.report(result),
                    "REPORT differs from extraction")
    print(json.dumps({"status": "PASS", "optimized": not __debug__,
                      "mutation_checks": checks}, sort_keys=True))


if __name__ == "__main__":
    main()
