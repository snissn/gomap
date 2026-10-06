"""Describe sequential before/after C2 packets; never grant cost acceptance."""
import argparse
import datetime
import hashlib
import json
from pathlib import Path


def read(path):
    return json.loads(path.read_text())


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(packet, kind):
    validation = read(packet / "analysis-validation.json")
    expected = {"raw": (27, 648, 216), "mvcc": (18, 108, 36)}[kind]
    identity = read(packet / "source-identity.json")
    policy = read(packet / "policy.json")
    completion = read(packet / "completion.json")
    receipts = read(packet / "receipts.json")
    assert completion["runs"] == expected[0] and not completion["source_drift"]
    assert len(receipts) == expected[0] and validation["rows"] == expected[1]
    assert validation["matched_cases"] == expected[2]
    assert validation["raw_hashes_verified"] and validation["stderr_hashes_verified"]
    assert policy["source_tree_sha256"] == identity["source_tree_sha256"]
    assert validation["source_tree_sha256"] == identity["source_tree_sha256"]
    for receipt in receipts:
        assert receipt["exit_code"] == 0 and not receipt["source_drift_after"]
        assert receipt["case_validation"] and not receipt["validation_error"]
        assert sha(packet / (receipt["label"] + ".log")) == receipt["log_sha256"]
        assert sha(packet / (receipt["label"] + ".stderr.log")) == receipt["stderr_sha256"]
    fields = ("profile", "mode", "layout", "records", "operation") if kind == "raw" else ("profile", "mode", "read", "iterations")
    rows = read(packet / "matched-summary.json")
    lookup = {tuple(row[k] for k in fields): row for row in rows}
    assert len(lookup) == len(rows) == expected[2]
    return identity, policy, fields, lookup


def compare(before, after, kind):
    old_identity, old_policy, fields, old = load(before, kind)
    new_identity, new_policy, new_fields, new = load(after, kind)
    assert fields == new_fields and old.keys() == new.keys()
    assert old_identity["commit"] != new_identity["commit"]
    shared = ("fixture_sha256", "orders", "GOMAXPROCS", "host", "cpu_count")
    counts = ("FreshCapture_operations", "other_operations", "durability_operations", "fixtures") if kind == "raw" else ("operations", "logical_keys", "value_bytes", "timed_scope", "profile_acknowledgement")
    for key in shared + counts:
        assert old_policy[key] == new_policy[key], "changed collection contract: " + key
    assert all(value is None for value in old_policy["original_memory_controls_removed"].values())
    assert all(value is None for value in new_policy["original_memory_controls_removed"].values())
    comparisons = []
    for key in sorted(old):
        row = dict(zip(fields, key))
        row["metrics"] = {}
        for metric in ("ns/op", "B/op", "allocs/op"):
            a = old[key]["metrics"][metric]
            b = new[key]["metrics"][metric]
            assert len(a["values"]) == len(b["values"]) == 3
            row["metrics"][metric] = {
                "before": a, "after": b,
                "median_delta": b["median"] - a["median"],
                "after_over_before_median": b["median"] / a["median"] if a["median"] else None,
                "zero_denominator": not bool(a["median"]),
            }
        comparisons.append(row)
    bindings = {}
    for label, packet in (("before", before), ("after", after)):
        bindings[label] = {name: sha(packet / name) for name in (
            "source-identity.json", "policy.json", "completion.json", "receipts.json",
            "analysis-validation.json", "matched-summary.json")}
    return {"before_candidate": old_identity["commit"], "after_candidate": new_identity["commit"],
            "before_source_tree_sha256": old_identity["source_tree_sha256"],
            "after_source_tree_sha256": new_identity["source_tree_sha256"],
            "fixture_sha256": new_policy["fixture_sha256"],
            "input_bindings": bindings, "comparisons": comparisons}


def main():
    parser = argparse.ArgumentParser()
    for name in ("before-raw", "before-mvcc", "after-raw", "after-mvcc", "out"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    raw = compare(args.before_raw, args.after_raw, "raw")
    mvcc = compare(args.before_mvcc, args.after_mvcc, "mvcc")
    assert raw["before_candidate"] == mvcc["before_candidate"]
    assert raw["after_candidate"] == mvcc["after_candidate"]
    assert raw["before_source_tree_sha256"] == mvcc["before_source_tree_sha256"]
    assert raw["after_source_tree_sha256"] == mvcc["after_source_tree_sha256"]
    result = {"created_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "kind": "descriptive sequential source-matched C2 minimization comparison",
              "whole_C2_cost_acceptance": False,
              "limits": ["candidate packets are sequential, not interleaved before/after A/B",
                         "three bounded repeats; no significance or sustained C4 claim",
                         "input raw streams and validation hashes verified; no new functional acceptance",
                         "explicit correctness-required residual cost disposition remains necessary"],
              "raw": raw, "mvcc": mvcc}
    args.out.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"raw_cases": len(raw["comparisons"]), "mvcc_cases": len(mvcc["comparisons"]),
                      "before": raw["before_candidate"], "after": raw["after_candidate"],
                      "whole_C2_cost_acceptance": False}))


if __name__ == "__main__":
    main()
