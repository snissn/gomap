#!/usr/bin/env python3
"""Strict parser copied from ycsb_compare_validate.py at4988738; no runtime imports."""
import re
import math

def require(condition, message):
    if not condition:
        raise ValueError(message)

NUM = r"[0-9]+(?:\.[0-9]+)?"
ROW = re.compile(r"^([A-Z_]+)\s+- Takes\(s\): (" + NUM + r"), Count: ([0-9]+), OPS: (" + NUM + r"), Avg\(us\): (" + NUM + r"), Min\(us\): (" + NUM + r"), Max\(us\): (" + NUM + r"), 50th\(us\): (" + NUM + r"), 90th\(us\): (" + NUM + r"), 95th\(us\): (" + NUM + r"), 99th\(us\): (" + NUM + r"), 99.9th\(us\): (" + NUM + r"), 99.99th\(us\): (" + NUM + r")$")

def parse_log(path):
    # The pinned go-ycsb emits cumulative progress summaries from a ticker.
    # Its final summary is emitted only after the ticker goroutine has stopped,
    # following the unique "Run finished" marker.  Progress is not another
    # completed sample; do not choose a row by occurrence or overwrite it.
    rows = {}
    text = path.read_text(encoding="utf-8")
    require(text.endswith("\n"), f"truncated output in {path.name}")
    finished = False
    progress = {}
    for line in text.splitlines():
        if line.lstrip().startswith("Run finished"):
            require(re.fullmatch(r"Run finished, takes (?:[0-9]+(?:\.[0-9]+)?(?:h|ms|m|s|µs|us|ns))+", line) is not None, f"malformed final marker in {path.name}")
            require(not finished, f"duplicate final marker in {path.name}")
            finished = True
            continue
        match = ROW.fullmatch(line)
        if not match:
            require(not re.match(r"^\s*[A-Z_]+\s+-", line) and " - Takes" not in line and "Takes(s):" not in line, f"malformed measurement row in {path.name}")
            continue
        op, takes, count, ops, avg, minimum, maximum, p50, p90, p95, p99, p999, p9999 = match.groups()
        # A nonzero error seen during progress cannot be made valid by a
        # later missing or inconsistent final error row.
        if not finished:
            require(not op.endswith("_ERROR") or int(count) == 0, f"operation errors in progress in {path.name}")
            require(int(count) >= progress.get(op, 0), f"decreasing progress count in {path.name}")
            progress[op] = int(count)
            continue
        require(all(math.isfinite(float(n)) for n in (takes,ops,avg,minimum,maximum,p50,p90,p95,p99,p999,p9999)), f"invalid nonfinite measurement in {path.name}")
        require(op not in rows, f"duplicate {op} row in {path.name}")
        rows[op] = dict(operation=op, count=int(count), ops_per_sec=float(ops), avg_us=float(avg), p50_us=float(p50), p95_us=float(p95), p99_us=float(p99), p999_us=float(p999), max_us=float(maximum))
    require(finished, f"missing final marker in {path.name}")
    require(all(op in rows and rows[op]["count"] >= count for op, count in progress.items()), f"final rows do not cover progress in {path.name}")
    return list(rows.values())


def check_counts(parsed, expected, phase, path):
    rows = {row["operation"]: row for row in parsed}
    require("TOTAL" in rows, f"missing TOTAL in {path.name}")
    allowed = {"INSERT", "INSERT_ERROR", "TOTAL"} if phase == "load" else {"READ", "READ_ERROR", "UPDATE", "UPDATE_ERROR", "TOTAL"}
    require(set(rows) <= allowed, f"unexpected workload operations in {path.name}")
    required = {"INSERT"} if phase == "load" else {"READ", "UPDATE"}
    require(required <= rows.keys(), f"missing required measured operations in {path.name}")
    require(all(rows[op]["count"] > 0 for op in required), f"empty required measured operation in {path.name}")
    require(rows["TOTAL"]["count"] == expected, f"TOTAL count mismatch in {path.name}")
    successful = [row for op, row in rows.items() if op != "TOTAL" and not op.endswith("_ERROR")]
    require(successful and sum(row["count"] for row in successful) == expected, f"operation count mismatch in {path.name}")
    require(all(row["count"] == 0 for op, row in rows.items() if op.endswith("_ERROR")), f"operation errors in {path.name}")
