"""Validate and summarize the exact-source bounded C2 cost collection.

Retain every raw sample. Three fixed-count repeats do not establish statistical
significance or sustained qualification; fixture charges are not per-op heap.
"""
import argparse
import hashlib
import itertools
import json
import math
from pathlib import Path
import re
import statistics


PROFILES = ("command_wal_durable", "command_wal_relaxed", "no_wal_fast")
MODES = ("append_only", "btree", "cow_btree")
LAYOUTS = ("Inline64", "Pointer4096")
OPERATIONS = ("FreshCapture", "CaptureReadRelease", "Forward16", "IncrementalWrite",
              "IncrementalWriteSync")
GROUP_OPERATIONS = {"fresh": OPERATIONS[:1], "other": OPERATIONS[1:4],
                    "durability": OPERATIONS[4:]}
BASE_METRICS = ("ns/op", "B/op", "allocs/op", "p50-ns", "p95-ns", "p99-ns",
                "snapshot_rotations/op", "warm_snapshot_rotations")
COUNTER_METRICS = ("fixture_checkpoint_runs", "fixture_command_wal_sync_count",
                   "fixture_command_wal_file_sync_calls", "fixture_value_log_sync_calls")
COW_METRICS = tuple("fixture_cow_" + key for key in (
    "total_bytes", "history_bytes", "reserved_bytes", "retired_bytes", "peak_bytes",
    "control_bytes", "deferred_bytes", "external_bytes", "views", "generations",
    "sources", "external_leases", "active_cuts", "capture_calls_total",
    "prepare_calls_total", "publications_total", "rollovers_total", "handoffs_total",
    "current_roots", "frozen_roots"))
CLOSE_KEYS = {"total", "history", "reserved", "retired", "controls", "external",
              "generations", "sources", "views", "leases", "peak"}
NAME = re.compile(r"^BenchmarkCOWPublicDirtyCost/([^/]+)/([^/]+)/([^/]+)/N=(\d+)/([^\s]+?)-\d+\s+(.*)$")


def parse_row(line):
    matched = NAME.match(line)
    if not matched:
        return None
    profile, mode, layout, records, operation, rest = matched.groups()
    tokens = rest.split()
    if len(tokens) < 3 or (len(tokens) - 1) % 2:
        raise ValueError("invalid benchmark metric pairs: " + line)
    metrics = {}
    for index in range(1, len(tokens), 2):
        unit = tokens[index + 1]
        if unit in metrics:
            raise ValueError("duplicate metric " + unit)
        metrics[unit] = float(tokens[index])
    missing = set(BASE_METRICS + COUNTER_METRICS) - set(metrics)
    if missing:
        raise ValueError("missing metrics " + repr(missing))
    if any(not math.isfinite(value) or value < 0 for value in metrics.values()):
        raise ValueError("non-finite or negative benchmark metric")
    if mode == "cow_btree" and set(COW_METRICS) - set(metrics):
        raise ValueError("missing complete authoritative COW fixture metrics")
    return dict(profile=profile, mode=mode, layout=layout, records=int(records),
                operation=operation, iterations=int(tokens[0]), metrics=metrics)


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("packet", type=Path)
    args = parser.parse_args()
    packet = args.packet.resolve()
    receipts = json.loads((packet / "receipts.json").read_text())
    completion = json.loads((packet / "completion.json").read_text())
    if len(receipts) != 27 or completion["runs"] != 27 or completion["source_drift"]:
        raise ValueError("incomplete or drifted collection")
    expected_labels = {f"r{repeat}-{mode}-{group}" for repeat in (1, 2, 3)
                       for mode in MODES for group in GROUP_OPERATIONS}
    labels = [receipt["label"] for receipt in receipts]
    if len(set(labels)) != len(labels) or set(labels) != expected_labels:
        raise ValueError("missing, duplicate or extra raw run labels")
    rows, close_receipts = [], []
    for receipt in receipts:
        if receipt["exit_code"] or receipt["source_drift_after"]:
            raise ValueError("failed or drifted raw run: " + receipt["label"])
        label = receipt["label"]
        repeat, mode, group = label.split("-", 2)
        content = (packet / (label + ".log")).read_bytes()
        if hashlib.sha256(content).hexdigest() != receipt["log_sha256"]:
            raise ValueError("raw log hash mismatch: " + label)
        stderr = (packet / (label + ".stderr.log")).read_bytes()
        if hashlib.sha256(stderr).hexdigest() != receipt["stderr_sha256"]:
            raise ValueError("raw stderr hash mismatch: " + label)
        if re.search(rb"WARNING: DATA RACE|fatal error:", stderr):
            raise ValueError("runtime failure in stderr: " + label)
        text = content.decode()
        if not re.search(r"^PASS$", text, re.M):
            raise ValueError("no package pass: " + label)
        found = []
        run_close_count = 0
        for line in text.splitlines():
            if "COW_FINAL_CLOSE" in line:
                values = {key: int(value) for key, value in
                          re.findall(r"([a-z]+)=(\d+)", line)}
                if set(values) != CLOSE_KEYS:
                    raise ValueError("incomplete final close receipt: " + label)
                if mode != "cow_btree":
                    raise ValueError("COW close receipt in comparator run: " + label)
                if values["peak"] <= 0:
                    raise ValueError("vacuous nil-authority close receipt: " + label)
                run_close_count += 1
                if any(value for key, value in values.items() if key != "peak"):
                    raise ValueError("nonzero final live charge: " + label)
                close_receipts.append(dict(run=label, values=values))
            row = parse_row(line)
            if row is None:
                continue
            row.update(repeat=int(repeat[1:]), run=label)
            expected_count = 128 if group == "other" else 16
            if row["iterations"] != expected_count or row["mode"] != mode:
                raise ValueError("unmatched run policy: " + label)
            if mode == "cow_btree":
                if row["metrics"]["snapshot_rotations/op"] or row["metrics"]["warm_snapshot_rotations"]:
                    raise ValueError("COW snapshot rotation: " + label)
                if "fixture_cow_total_bytes" not in row["metrics"]:
                    raise ValueError("missing authoritative fixture accounting: " + label)
            found.append(row)
        operations = GROUP_OPERATIONS[group]
        expected = set(itertools.product(PROFILES, (mode,), LAYOUTS, (1024, 2048), operations))
        actual = {(r["profile"], r["mode"], r["layout"], r["records"], r["operation"]) for r in found}
        if actual != expected or len(found) != len(expected):
            raise ValueError("missing, duplicate or extra benchmark cases: " + label)
        # Go may retain the initial 1x warmup cleanup as well as fixed-count
        # cleanup. Require at least one complete close receipt per COW case.
        if mode == "cow_btree" and run_close_count < len(expected):
            raise ValueError("missing final COW close receipts: " + label)
        rows.extend(found)
    if len(rows) != 540:
        raise ValueError("expected 540 exact matched rows")

    grouped = {}
    for row in rows:
        key = tuple(row[k] for k in ("profile", "mode", "layout", "records", "operation"))
        grouped.setdefault(key, []).append(row)
    summaries = []
    for key, group in sorted(grouped.items()):
        if {r["repeat"] for r in group} != {1, 2, 3} or len(group) != 3:
            raise ValueError("unmatched repeats " + str(key))
        summary = dict(zip(("profile", "mode", "layout", "records", "operation"), key))
        summary["metrics"] = {}
        for unit in sorted(group[0]["metrics"]):
            values = [r["metrics"][unit] for r in group]
            median = statistics.median(values)
            summary["metrics"][unit] = dict(values=values, median=median,
                minimum=min(values), maximum=max(values),
                max_over_min=(max(values) / min(values) if min(values) > 0 else None))
        summaries.append(summary)
    lookup = {(r["repeat"], r["profile"], r["mode"], r["layout"], r["records"], r["operation"]): r for r in rows}
    ratios = []
    for profile, layout, records, operation, baseline in itertools.product(
            PROFILES, LAYOUTS, (1024, 2048), OPERATIONS, MODES[:2]):
        item = dict(profile=profile, layout=layout, records=records,
                    operation=operation, numerator="cow_btree", denominator=baseline)
        item["metrics"] = {}
        for unit in ("ns/op", "B/op", "allocs/op"):
            values = []
            for repeat in (1, 2, 3):
                cow = lookup[(repeat, profile, "cow_btree", layout, records, operation)]["metrics"][unit]
                base = lookup[(repeat, profile, baseline, layout, records, operation)]["metrics"][unit]
                values.append(cow / base if base else None)
            item["metrics"][unit] = dict(values=values,
                median=(statistics.median(values) if all(v is not None for v in values) else None))
        ratios.append(item)
    scaling = []
    for profile, mode, layout, operation in itertools.product(PROFILES, MODES, LAYOUTS, OPERATIONS):
        item = dict(profile=profile, mode=mode, layout=layout, operation=operation,
                    numerator_records=2048, denominator_records=1024, metrics={})
        for unit in ("ns/op", "B/op", "allocs/op"):
            values = []
            for repeat in (1, 2, 3):
                large = lookup[(repeat, profile, mode, layout, 2048, operation)]["metrics"][unit]
                small = lookup[(repeat, profile, mode, layout, 1024, operation)]["metrics"][unit]
                values.append(large / small if small else None)
            item["metrics"][unit] = dict(values=values,
                median=(statistics.median(values) if all(v is not None for v in values) else None))
        scaling.append(item)
    write(packet / "rows.json", rows)
    write(packet / "matched-summary.json", summaries)
    write(packet / "cow-over-comparators.json", ratios)
    write(packet / "N2048-over-N1024.json", scaling)
    write(packet / "close-charge-receipts.json", close_receipts)
    write(packet / "analysis-validation.json", dict(raw_runs=27, rows=len(rows),
        matched_cases=len(summaries), raw_hashes_verified=True, stderr_hashes_verified=True,
        source_tree_sha256=json.loads((packet / "source-identity.json").read_text())["source_tree_sha256"],
        scope="bounded C2 diagnostic; no statistical significance or sustained C4 acceptance",
        fixture_charge_scope="whole fixture setup/reseeds/last observed cut outside timer; not per-op heap",
        close_receipts=len(close_receipts),
        tail_scope="16/128 fixed samples per case; descriptive percentiles only",
        rss_scope="whole child run includes setup/teardown; receipts are not per-op memory"))
    print(json.dumps(dict(rows=len(rows), cases=len(summaries), close_receipts=len(close_receipts))))


if __name__ == "__main__":
    main()
