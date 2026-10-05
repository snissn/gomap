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
