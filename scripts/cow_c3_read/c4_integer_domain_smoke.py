"""Serialized copies of actual C4 receipts exercise Go integer wire domains."""
import argparse
import copy
import gzip
import json
from pathlib import Path

from c4_protocol import COW_COUNTERS, LIMITS, case_names, load, validate_config, validate_raw, workload
from protocol import sha, write

UINT64_MAX = (1 << 64) - 1
INT64_MAX = (1 << 63) - 1
BASE_COUNTERS = ("treedb.command_wal.append.count_total", "treedb.command_wal.file_sync.calls_total",
                 "treedb.cache.checkpoint.runs", "treedb.commit_seq", "treedb.cache.snapshot.rotations_total",
                 "treedb.cache.snapshot.rotated_shards_total", "treedb.cache.snapshot.enqueued_records_total")


def set_counter(raw, key, value):
    for boundary in raw["boundaries"]:
        if boundary["stats"]:
            boundary["stats"][key] = value


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--positive", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    config = load(args.positive / "config.json")
    validate_config(config)
    receipts = load(args.positive / "receipts.json")
    cases = {case["id"]: case for case in config["cases"]}
    results, controls, failures = [], [], []
    selected = {}
    for mode in ("append_only", "btree", "cow_btree"):
        receipt = next(item for item in receipts if cases[item["case"]]["mode"] == mode
                       and cases[item["case"]]["layout"] == "forced_pointer")
        file = args.positive / (receipt["label"] + "-lifecycle") / receipt["raw_lifecycles"][-1]["path"]
        raw = load(file)
        case = cases[receipt["case"]]
        assert sha(file) == receipt["raw_lifecycles"][-1]["sha256"]
        validation = validate_raw(raw, case, raw["epochs"])
        selected[mode] = (raw, case)
        controls.append({"mode": mode, "path": str(file), "sha256": sha(file), "validation": validation})

    def check(name, raw, case, expected=None):
        file = args.out / (name + ".json")
        write(file, raw)
        try:
            validate_raw(load(file), case, raw["epochs"])
        except ValueError as error:
            result = {"case": name, "refused": str(error), "sha256": sha(file)}
            if expected is None or expected not in str(error):
                failures.append(result)
        else:
            result = {"case": name, "accepted": True, "sha256": sha(file)}
            if expected is not None:
                failures.append(result)
        # Keep the exact serialized input without multiplying large packet copies.
        compressed = file.with_suffix(".json.gz")
        with gzip.open(compressed, "wb") as stream:
            stream.write(file.read_bytes())
        assert gzip.decompress(compressed.read_bytes()) == file.read_bytes()
        file.unlink()
        result["artifact"] = compressed.name
        results.append(result)

    cow, cow_case = selected["cow_btree"]
    for key in LIMITS:
        raw = copy.deepcopy(cow)
        raw["limits"][key] = float(raw["limits"][key])
        check("float-limit-" + key, raw, cow_case, "raw limit mismatch " + key)
    for name, change in (("missing-limit", lambda value: value.pop("MaxViews")),
                         ("extra-limit", lambda value: value.update(Unexpected=1)),
                         ("bool-limit", lambda value: value.update(MaxViews=True))):
        raw = copy.deepcopy(cow)
        change(raw["limits"])
        check(name, raw, cow_case, "limit")
    for mode, (original, case) in selected.items():
        keys = BASE_COUNTERS + (tuple("treedb.cache.cow." + key for key in COW_COUNTERS) if mode == "cow_btree" else ())
        for key in keys:
            raw = copy.deepcopy(original)
            raw["boundaries"][0]["stats"][key] = str(UINT64_MAX + 1)
            check(mode + "-overflow-" + key, raw, case, "out-of-range required counter " + key)
    for key in cow["layout_proofs"][0]["owners_before"]:
        raw = copy.deepcopy(cow)
        # Preserve the diagnostic equality so only the wire domain changes.
        for field in ("owners_before", "owners_after"):
            raw["layout_proofs"][0][field][key] = str(UINT64_MAX + 1)
        check("overflow-layout-owner-" + key, raw, cow_case, "out-of-range representation owner counter " + key)
    original, case = selected["append_only"]
    for value, expected in ((UINT64_MAX, None), (UINT64_MAX + 1, "out-of-range required counter treedb.commit_seq")):
        raw = copy.deepcopy(original)
        set_counter(raw, "treedb.commit_seq", str(value))
        check("backend-sequence-" + str(value), raw, case, expected)
    key = "treedb.cache.vlog_payload_kind.raw_bytes.single_value"
    for value, expected in ((UINT64_MAX, None), (UINT64_MAX + 1, "out-of-range persistent single-value counter")):
        raw = copy.deepcopy(original)
        next(item for item in raw["boundaries"] if item["phase"] == "pinned_checkpoint")["stats"][key] = str(value)
        check("pointer-counter-" + str(value), raw, case, expected)
    for value, expected in ((INT64_MAX, None), (INT64_MAX + 1, "out-of-range call timing")):
        raw = copy.deepcopy(original)
        shift = value - max(call["completion_ns"] for call in raw["calls"])
        for call in raw["calls"]:
            call["start_ns"] += shift
            call["completion_ns"] += shift
        check("call-time-" + str(value), raw, case, expected)
    for name in ("float-cycles", "float-key-count"):
        changed = copy.deepcopy(config)
        if name == "float-cycles":
            changed["cycles"] = 3.0
        else:
            # Rebind all derived configuration identities around the float key count.
            changed_case = changed["cases"][0]
            changed_case["keys"] = float(changed_case["keys"])
            changed_case["id"], changed_case["benchmark"] = case_names(changed_case)
            changed_case["workload_contract"] = workload(changed_case["keys"], changed_case["iterations"])
        file = args.out / (name + ".json")
        write(file, changed)
        try:
            validate_config(load(file))
        except ValueError as error:
            result = {"case": name, "refused": str(error), "sha256": sha(file)}
        else:
            result = {"case": name, "accepted": True, "sha256": sha(file)}
            failures.append(result)
        results.append(result)
    write(args.out / "result.json", {"scope": "Historical serialized receipt parser checks only; no new source, execution or performance admission",
        "controls": controls, "results": results, "failures": failures,
        "script_sha256": sha(Path(__file__)), "protocol_sha256": sha(Path(__file__).with_name("c4_protocol.py"))})
    print(json.dumps({"controls": len(controls), "checks": len(results), "failures": len(failures)}))
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
