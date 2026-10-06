"""Fail-closed validation and descriptive matched summaries for actual MVCC."""
import argparse
import hashlib
import itertools
import json
import math
from pathlib import Path
import re
import statistics

from analyze import PROFILES, MODES, COW_METRICS, write


NAME = re.compile(r"^BenchmarkCOWIntegratedPublicMVCC/([^/]+)/([^/]+)/([^\s]+?)-\d+\s+(.*)$")
BASE = {"ns/op", "B/op", "allocs/op", "p50_ns", "p95_ns", "p99_ns", "output/op",
        "snapshot_rotations/op", "rotated_shards/op", "enqueued_records/op",
        "heap_end_B", "process_alloc_B/op"}


def parse_row(line):
    matched = NAME.match(line)
    if not matched:
        return None
    profile, mode, read, rest = matched.groups()
    tokens = rest.split()
    if len(tokens) < 3 or (len(tokens) - 1) % 2:
        raise ValueError("invalid metric pairs")
    metrics = {}
    for index in range(1, len(tokens), 2):
        unit = tokens[index + 1]
        if unit in metrics:
            raise ValueError("duplicate metric " + unit)
        metrics[unit] = float(tokens[index])
    required = BASE | ({"visited/op"} if read == "all_versions" else set())
    if profile != "no_wal_fast":
        required |= {"wal_appends/op", "wal_syncs/op"}
    if mode == "cow_btree":
        required |= set(COW_METRICS)
    if required - metrics.keys() or any(not math.isfinite(v) or v < 0 for v in metrics.values()):
        raise ValueError("missing, nonfinite or negative metrics")
    count = int(tokens[0])
    if count not in (128, 256):
        raise ValueError("unexpected operation count")
    output = 1 if read == "point" else (count / 8 + 1) / 2
    if metrics["output/op"] != output or (read == "all_versions" and metrics["visited/op"] != output):
        raise ValueError("incomplete history/output")
    if mode == "cow_btree" and (metrics["snapshot_rotations/op"] or metrics["rotated_shards/op"]):
        raise ValueError("COW snapshot rotation")
    return dict(profile=profile, mode=mode, read=read, iterations=count, metrics=metrics)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("packet", type=Path)
    packet = parser.parse_args().packet.resolve()
    receipts = json.loads((packet / "receipts.json").read_text())
    completion = json.loads((packet / "completion.json").read_text())
    expected_labels = {f"r{r}-{m}-N{n}" for r, m, n in itertools.product((1, 2, 3), MODES, (128, 256))}
    if len(receipts) != 18 or completion["runs"] != 18 or completion["source_drift"]:
        raise ValueError("incomplete or drifted collection")
    if {r["label"] for r in receipts} != expected_labels:
        raise ValueError("unmatched labels")
    rows = []
    for receipt in receipts:
        label = receipt["label"]
        repeat, mode, count = label.split("-")
        count = int(count[1:])
        raw = (packet / (label + ".log")).read_bytes()
        if receipt["exit_code"] or receipt["source_drift_after"] or hashlib.sha256(raw).hexdigest() != receipt["log_sha256"]:
            raise ValueError("failed, drifted or corrupted run " + label)
        stderr = (packet / (label + ".stderr.log")).read_bytes()
        if hashlib.sha256(stderr).hexdigest() != receipt["stderr_sha256"]:
            raise ValueError("raw stderr hash mismatch " + label)
        if re.search(rb"WARNING: DATA RACE|fatal error:", stderr):
            raise ValueError("runtime failure in stderr " + label)
        text = raw.decode()
        if not re.search(r"^PASS$", text, re.M):
            raise ValueError("missing PASS " + label)
        found = [row for line in text.splitlines() if (row := parse_row(line)) is not None]
        expected = set(itertools.product(PROFILES, (mode,), ("point", "all_versions"), (count,)))
        actual = {(r["profile"], r["mode"], r["read"], r["iterations"]) for r in found}
        if actual != expected or len(found) != 6:
            raise ValueError("missing, duplicate or extra cases " + label)
        for row in found:
            row.update(repeat=int(repeat[1:]), run=label)
        rows.extend(found)
    grouped = {}
    for row in rows:
        key = tuple(row[k] for k in ("profile", "mode", "read", "iterations"))
        grouped.setdefault(key, []).append(row)
    summaries = []
    for key, group in sorted(grouped.items()):
        if len(group) != 3 or {r["repeat"] for r in group} != {1, 2, 3}:
            raise ValueError("unmatched repeats")
        summary = dict(zip(("profile", "mode", "read", "iterations"), key))
        summary["metrics"] = {}
        for unit in sorted(group[0]["metrics"]):
            values = [r["metrics"][unit] for r in group]
            summary["metrics"][unit] = dict(values=values, median=statistics.median(values),
                                            minimum=min(values), maximum=max(values),
                                            max_over_min=max(values) / min(values) if min(values) else None)
        summaries.append(summary)
    lookup = {(r["repeat"], r["profile"], r["mode"], r["read"], r["iterations"]): r for r in rows}
    ratios = []
    for profile, read, count, base in itertools.product(PROFILES, ("point", "all_versions"), (128, 256), MODES[:2]):
        item = dict(profile=profile, read=read, iterations=count, numerator="cow_btree", denominator=base, metrics={})
        for unit in ("ns/op", "B/op", "allocs/op"):
            values = []
            for repeat in (1, 2, 3):
                cow = lookup[(repeat, profile, "cow_btree", read, count)]["metrics"][unit]
                baseline = lookup[(repeat, profile, base, read, count)]["metrics"][unit]
                values.append(cow / baseline if baseline else None)
            item["metrics"][unit] = dict(values=values, median=statistics.median(values) if all(v is not None for v in values) else None)
        ratios.append(item)
    write(packet / "rows.json", rows)
    write(packet / "matched-summary.json", summaries)
    write(packet / "cow-over-comparators.json", ratios)
    write(packet / "analysis-validation.json", dict(raw_runs=18, rows=len(rows), matched_cases=len(summaries),
        raw_hashes_verified=True, stderr_hashes_verified=True,
        source_tree_sha256=json.loads((packet / "source-identity.json").read_text())["source_tree_sha256"],
        scope="tiny growing-history C2 public-MVCC diagnostic; fences retained; no sustained, significance, C3 or C4 claim",
        complete_history_outputs={"128": 8.5, "256": 16.5}, final_close_drain="validated separately by root-package dirty fixture"))
    print(json.dumps(dict(rows=len(rows), cases=len(summaries))))


if __name__ == "__main__":
    main()
