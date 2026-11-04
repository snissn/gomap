#!/usr/bin/env python3
"""Fixed twelve process-usage receipts; no profile decoder or gate estimator."""
import json
from pathlib import Path
import sys

COUNTERS = ("user_cpu_ns", "system_cpu_ns", "minor_faults", "major_faults",
            "voluntary_context_switches", "involuntary_context_switches")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def validate(path):
    require(path.is_file() and not path.is_symlink() and path.stat().st_size <= 4096,
            "missing, linked or oversized receipt")
    value = json.loads(path.read_text())
    require(value["schema"] == 1 and value["scope"] == "WORK_INTERVAL" and
            value["completed"] is True and
            value["status"] == "COMPLETE_WORK_INTERVAL_DIAGNOSTIC_ONLY" and
            value["read_attempts"] == 2, "incomplete process interval")
    samples = value["samples"]
    require(isinstance(samples, list) and len(samples) == 2, "sample population")
    for sample in samples:
        require(sample["read_failed"] is False, "failed read")
        for field in ("read_started_unix_ns", "read_finished_unix_ns", "read_wall_ns"):
            require(type(sample[field]) is int and sample[field] >= 0, "invalid read wall bound")
        require(sample["read_finished_unix_ns"] >= sample["read_started_unix_ns"], "read wall order")
        require(set(sample["counters"]) == set(COUNTERS), "counter inventory")
        for field in COUNTERS:
            require(type(sample["counters"][field]) is int and sample["counters"][field] >= 0,
                    "invalid process counter")
    require(samples[1]["read_started_unix_ns"] >= samples[0]["read_finished_unix_ns"],
            "read interval order")
    lower, upper = value["interval_wall_lower_ns"], value["interval_wall_upper_ns"]
    require(type(lower) is int and type(upper) is int and 0 <= lower <= upper,
            "invalid monotonic wall interval")
    require(set(value["delta"]) == set(COUNTERS), "delta inventory")
    for field in COUNTERS:
        delta = samples[1]["counters"][field] - samples[0]["counters"][field]
        require(type(value["delta"][field]) is int and delta >= 0 and value["delta"][field] == delta,
                "counter regression or wrong delta")
    return value


def main(root):
    results, errors = [], []
    for sample in range(1, 7):
        for mode in (("off", "auto") if sample % 2 else ("auto", "off")):
            name = f"vlog_auto_incompressible_{mode}_sample{sample}.rusage.json"
            row = {"sample": sample, "mode": mode, "receipt": name}
            try:
                row["work_interval"] = validate(root / name)
            except (OSError, ValueError, KeyError, TypeError) as error:
                row["error"] = str(error)
                errors.append(name)
            results.append(row)
    print(json.dumps({"scope": "WORK_INTERVAL_ONLY_NOT_CHECKED_CLOSE_OR_PPROF_EQUIVALENCE",
                      "status": "TWELVE_INTERVALS_VALID" if not errors else "INCOMPLETE_INCONCLUSIVE",
                      "rows": results, "errors": errors}, sort_keys=True))
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1])))
