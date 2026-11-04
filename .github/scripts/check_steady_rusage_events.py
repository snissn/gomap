#!/usr/bin/env python3
"""Fixed Linux steady-rusage control inventory; raw events remain the authority."""
import json
from pathlib import Path
import sys

PACKAGE = "github.com/snissn/gomap/cmd/unified_bench"
ROOT = Path("artifacts/steady-rusage")
MAX_BYTES = 32 * 1024 * 1024  # Post-run acceptance/read bound, not a live cap.
TOPS = (
    "TestRunBenchmarkProcessUsageDefaultZero",
    "TestRunBenchmarkProcessUsageTwoReadsCheckpointOrder",
    "TestRunBenchmarkProcessUsageOriginalFailureWins",
    "TestRunBenchmarkProcessUsageReaderError",
    "TestProcessUsageTimevalBounds",
    "TestProcessUsageSelectionRefusal",
    "TestProcessUsageCounterRegression",
    "TestProcessUsageIncompleteAndReadLimit",
    "TestProcessUsageSampleRegressionFailsClosed",
    "TestRunBenchmarkProcessUsageCloseFailureIsNotLifecycleSuccess",
    "TestRunBenchmarkBatchWriteSteadyCPUProfileMatchesTimedSpan",
)
CHILDREN = {
    "TestProcessUsageSelectionRefusal": (
        "unsupported", "relative", "multi_db", "multi_test", "no_cpu", "existing",
    ),
    "TestProcessUsageCounterRegression": (
        "user_cpu", "system_cpu", "minor_faults", "major_faults", "voluntary", "involuntary",
    ),
    "TestRunBenchmarkProcessUsageReaderError": ("first", "second"),
}
EXPECTED = frozenset(TOPS) | frozenset(
    parent + "/" + child for parent, children in CHILDREN.items() for child in children
)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def reject_constant(value):
    raise ValueError("nonfinite JSON constant: " + value)


def check(mode):
    if mode not in ("normal", "race"):
        raise ValueError("expected fixed normal or race mode")
    allowed = {"source.txt", "go-version.txt", "go-env.json"} | {
        name + suffix for name in ("normal", "race")
        for suffix in (".jsonl", ".stderr", ".exit", ".receipt.json")
    }
    total = 0
    for path in ROOT.iterdir():
        if path.name not in allowed or path.is_symlink() or not path.is_file():
            raise ValueError("unexpected output member")
        total += path.stat().st_size
        if total > MAX_BYTES:
            raise ValueError("aggregate output exceeds post-run 32MiB acceptance bound")
    if (ROOT / (mode + ".exit")).read_text() != "0\n":
        raise ValueError("literal Go command exit was not zero")
    states = {}
    package_started = False
    package_passed = False
    compiler_events = 0
    compiler_imports = {PACKAGE + " [" + PACKAGE + ".test]",
                        PACKAGE + "_test [" + PACKAGE + ".test]", PACKAGE + ".test"}
    with (ROOT / (mode + ".jsonl")).open() as stream:
        for number, line in enumerate(stream, 1):
            event = json.loads(line, object_pairs_hook=unique_object,
                               parse_constant=reject_constant)
            if isinstance(event, dict) and "ImportPath" in event:
                if (package_started or set(event) != {"Action", "ImportPath", "Output"}
                        or event["Action"] != "build-output"
                        or event["ImportPath"] not in compiler_imports
                        or not isinstance(event["Output"], str) or not event["Output"]):
                    raise ValueError("foreign/malformed/late compiler event")
                compiler_events += 1
                continue
            if not isinstance(event, dict) or event.get("Package") != PACKAGE:
                raise ValueError("unexpected package/event at line " + str(number))
            action, test = event.get("Action"), event.get("Test")
            output = event.get("Output", "")
            if not isinstance(output, str) or any(text in output for text in (
                    "panic:", "fatal error:", "DATA RACE", "test timed out", "[build failed]")):
                raise ValueError("failure diagnostic or malformed output")
            if action in ("fail", "skip"):
                raise ValueError("failed or skipped event at line " + str(number))
            if test is not None:
                if test not in EXPECTED or package_passed:
                    raise ValueError("unexpected or post-terminal test")
                if action == "run":
                    if not package_started or test in states:
                        raise ValueError("duplicate or unstarted test run")
                    states[test] = "run"
                elif action == "pass":
                    if states.get(test) != "run":
                        raise ValueError("test pass without exactly one run")
                    states[test] = "pass"
                elif action not in ("output", "pause", "cont") or states.get(test) != "run":
                    raise ValueError("unsupported or inactive test action")
            elif action == "start":
                if package_started:
                    raise ValueError("duplicate package start")
                package_started = True
            elif action == "pass":
                if not package_started or package_passed:
                    raise ValueError("duplicate or unstarted package pass")
                package_passed = True
            elif action != "output":
                raise ValueError("unsupported package action")
    if not package_passed or set(states) != EXPECTED or any(v != "pass" for v in states.values()):
        raise ValueError("missing entered/passed tests or package terminal")
    receipt = {"status": "STEADY_RUSAGE_CONTROLS_PASS", "mode": mode,
               "package": PACKAGE, "tops": 11, "outcomes": 25, "leaves": 22,
               "passed": sorted(states), "go_exit": 0, "compiler_events": compiler_events,
               "output_bound": "32MiB post-run acceptance only; no live cap claimed"}
    encoded = json.dumps(receipt, sort_keys=True) + "\n"
    if total + len(encoded.encode()) > MAX_BYTES:
        raise ValueError("receipt would exceed post-run acceptance bound")
    return encoded


if __name__ == "__main__":
    try:
        if len(sys.argv) != 2:
            raise ValueError("expected exactly one fixed mode")
        result = check(sys.argv[1])
    except (OSError, ValueError, TypeError, KeyError) as exc:
        print(json.dumps({"status": "STEADY_RUSAGE_CONTROLS_FAIL",
                          "reason": str(exc)[:512]}, sort_keys=True))
        raise SystemExit(1)
    print(result, end="")
