#!/usr/bin/env python3
"""Validate one actual-command-v2 report before startup attribution.

Contract source: 6bb182ac30d7bd99602243ebee0da8b7eb275257, harness blobs
1c9b4b74bc69f05d8cec41be0058e03080f8b3d5 (public command) and
3322ad4bdd2de9b340122c1b39323cd57a3189c6 (startup trace).
The collection manifest must independently bind these sources and the binaries;
matching JSON cannot authenticate its producer. This checks report counters and
lifecycle status, not returned-document semantics or original owner Close itself.
The Go producer owns those checks. No database-open time is inferred, terminal
is inclusive, and trace origin differs from the original reopen timer boundary.

Usage from scripts/: command_startup_trace_validate.py REPORT --command native
  --operation dense --http-binary-sha256 SHA --native-binary-sha256 SHA
Default requires all three diagnostic traces; --trace-mode disabled requires
omission. Failed/partial reports remain retained evidence but are rejected here.
"""
import argparse
import json
import math
import re
import sys

from ycsb_evidence_json import read

PHASES = ("seed", "serve", "reopen")
LIMIT = 4096
INT64_MAX = (1 << 63) - 1
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
UNAVAILABLE = ["database_open_duration", "cross_process_monotonic_milestones"]
EVIDENCE = ("Command HTTP/native startup diagnostic; instrumentation overhead "
            "included; terminal includes failure cleanup; not Mongo qualification "
            "or causal attribution")
BOUNDARIES = {
    "http_health": "HTTP health status 200 after body drain/close attempts; separate native listener not probed",
    "native_hello": "native connection plus Hello; Close attempted with existing error handling",
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value, label, minimum=0, maximum=None):
    require(type(value) is int and value >= minimum
            and (maximum is None or value <= maximum), f"invalid {label}")
    return value


def nanoseconds(value, label, minimum=0):
    return integer(value, label, minimum, maximum=INT64_MAX)


def number(value, label):
    require(type(value) in (int, float) and math.isfinite(value) and value > 0,
            f"invalid {label}")
    return value


def validate_result(report, command, operation, binary_hashes, operations):
    """Check reported completion separately from diagnostic trace accounting."""
    require(isinstance(report, dict), "report must be an object")
    require(command in ("http", "native"), "invalid expected command")
    require(operation in ("keyword", "hybrid", "dense", "get", "range", "crud"),
            "invalid expected operation")
    require(command != "native" or operation not in ("keyword", "hybrid"),
            "native transport does not implement Keyword/Hybrid")
    require(report.get("version") == "actual-command-v2", "unsupported report version")
    require(report.get("Command") == command and report.get("Operation") == operation,
            "report cell differs from frozen command/operation")
    require(report.get("Phase") == "complete", "incomplete report lifecycle")
    require("failure" not in report and "failed_operation" not in report,
            "failed operation/lifecycle")
    require(report.get("cleanup_errors", []) == [], "failed/malformed cleanup")
    require(report.get("Profile") == "command_wal_durable", "unexpected durability profile")
    require(report.get("MutationACK") == "nativewire AckSynced; seed HTTP uses selected durable profile",
            "unexpected mutation ACK contract")
    require(integer(operations, "expected operations", 1) == integer(report.get("Requested"), "Requested", 1)
            == integer(report.get("Completed"), "Completed", 1), "operation counts differ")
    require(integer(report.get("SeedRows"), "SeedRows") == 4096, "seed fixture differs")
    for kind in ("HTTP", "Native"):
        expected = binary_hashes.get(kind)
        require(isinstance(expected, str) and SHA256.fullmatch(expected) is not None,
                f"invalid frozen {kind} binary hash")
        require(report.get(kind + "BinarySHA256") == expected, f"{kind} binary hash differs")
    samples = report.get("OperationNanoseconds")
    require(isinstance(samples, list) and len(samples) == operations, "missing operation samples")
    for sample in samples:
        nanoseconds(sample, "operation duration")
    ordered = sorted(samples)
    latency = report.get("LatencyNanoseconds")
    require(isinstance(latency, dict), "missing latency summary")
    for key, quantile in (("P50", .50), ("P95", .95), ("P99", .99), ("Max", 1)):
        require(nanoseconds(latency.get(key), key) == ordered[math.ceil(quantile * operations) - 1],
                f"inconsistent latency {key}")
    duration = number(report.get("DurationSeconds"), "DurationSeconds")
    rate = number(report.get("OperationsPerSecond"), "OperationsPerSecond")
    require(math.isclose(rate, operations / duration, rel_tol=1e-12), "inconsistent operation rate")
    for key in ("CloseNanoseconds", "MaintenanceNanoseconds", "ReopenNanoseconds", "ReopenCloseNanoseconds"):
        nanoseconds(report.get(key), key, 1)
    verification = report.get("Verification")
    require(isinstance(verification, dict) and set(verification) == {"after_requests", "second_reopen"},
            "missing producer verification phases")
    require(all(isinstance(value, dict) and value for value in verification.values()),
            "missing producer verification payload")


def validate_trace(trace, probe_kind):
    require(isinstance(trace, dict), "missing/malformed required trace")
    require(set(trace) == {"status", "truncated", "unavailable", "evidence", "probe_boundary", "events", "totals"},
            "unexpected trace schema")
    require(trace.get("status") == "ready" and trace.get("truncated") is False,
            "trace incomplete, failed or truncated")
    require(trace.get("unavailable") == UNAVAILABLE and trace.get("evidence") == EVIDENCE,
            "trace evidence limits changed")
    require(trace.get("probe_boundary") == BOUNDARIES[probe_kind], "probe boundary differs")
    events = trace.get("events")
    require(isinstance(events, list) and 4 <= len(events) <= LIMIT, "missing/excess trace events")
    totals = {}
    previous_end = 0
    for index, event in enumerate(events):
        require(isinstance(event, dict), "malformed event")
        require({"kind", "start_ns", "end_ns"} <= set(event)
                <= {"kind", "start_ns", "end_ns", "error"}, "unexpected event schema")
        kind = event.get("kind")
        require(kind in ("prepared", "spawn", probe_kind, "poll_sleep", "terminal"), "unknown event kind")
        start = nanoseconds(event.get("start_ns"), "event start")
        end = nanoseconds(event.get("end_ns"), "event end")
        require(start <= end, "negative event duration")
        error = event.get("error", "")
        # Go replaces invalid UTF-8 bytes during JSON serialization. Each U+FFFD
        # could represent one original byte, so use the minimum source length.
        require(isinstance(error, str), "malformed event error")
        require(len(error.encode("utf-8")) - 2 * error.count("\ufffd") <= 256,
                "unbounded event error")
        if kind == "terminal":
            require(index == len(events) - 1 and start == 0 and end >= previous_end and not error,
                    "invalid inclusive terminal span")
        else:
            require(start >= previous_end, "overlapping/out-of-order trace events")
        previous_end = end
        total = totals.setdefault(kind, {"count": 0, "failures": 0, "nanoseconds": 0})
        total["count"] += 1
        total["failures"] += bool(error)
        total["nanoseconds"] += end - start
    kinds = [event["kind"] for event in events]
    require(kinds[:2] == ["prepared", "spawn"] and kinds[-2:] == [probe_kind, "terminal"],
            "missing producer preparation/spawn/final probe/terminal")
    require(events[0]["start_ns"] == events[0]["end_ns"], "preparation must be a milestone")
    require(all(not event.get("error") for event in (events[0], events[1], events[-2])),
            "failed successful startup phase")
    for index, event in enumerate(events[2:-1], start=2):
        expected = probe_kind if index % 2 == 0 else "poll_sleep"
        require(event["kind"] == expected, "probe/sleep sequence differs")
        if expected == probe_kind:
            require(bool(event.get("error")) == (index != len(events) - 2),
                    "missing failed probe or premature readiness")
        else:
            require(not event.get("error"), "poll sleep has error")
    actual = trace.get("totals")
    require(isinstance(actual, dict) and set(actual) == set(totals), "missing/extra total kind")
    for kind, expected in totals.items():
        require(isinstance(actual[kind], dict) and set(actual[kind]) == set(expected), "malformed total")
        for key, value in expected.items():
            validate_integer = nanoseconds if key == "nanoseconds" else integer
            require(validate_integer(actual[kind][key], f"{kind}.{key}") == value,
                    "event/total accounting differs")


def validate(report, command, operation, binary_hashes, operations=1000, trace_mode="required"):
    validate_result(report, command, operation, binary_hashes, operations)
    require(trace_mode in ("required", "disabled"), "invalid trace mode")
    if trace_mode == "disabled":
        require("startup_trace" not in report, "disabled trace must be omitted")
    else:
        traces = report.get("startup_trace")
        require(isinstance(traces, dict) and set(traces) == set(PHASES), "missing/extra startup phase")
        for phase in PHASES:
            probe = "http_health" if phase == "seed" or command == "http" else "native_hello"
            validate_trace(traces[phase], probe)
    return {"status": "valid", "trace_mode": trace_mode,
            "scope": "trace accounting and reported operation/lifecycle status only",
            "unavailable": UNAVAILABLE}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report")
    parser.add_argument("--command", choices=("http", "native"), required=True)
    parser.add_argument("--operation", choices=("keyword", "hybrid", "dense", "get", "range", "crud"), required=True)
    parser.add_argument("--http-binary-sha256", required=True)
    parser.add_argument("--native-binary-sha256", required=True)
    parser.add_argument("--operations", type=int, default=1000)
    parser.add_argument("--trace-mode", choices=("required", "disabled"), default="required")
    args = parser.parse_args()
    try:
        result = validate(read(args.report), args.command, args.operation,
                          {"HTTP": args.http_binary_sha256, "Native": args.native_binary_sha256},
                          args.operations, args.trace_mode)
    except (ValueError, KeyError, TypeError, OSError, OverflowError) as error:
        print(f"invalid command startup report: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, allow_nan=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
