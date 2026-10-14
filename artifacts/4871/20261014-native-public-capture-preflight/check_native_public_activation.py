#!/usr/bin/env python3
"""Check the existing protected public fixture's activation evidence.

This supplements the byte-identical fixture; it does not qualify physical Finish,
the complete native budget inventory, performance, or the final graph.
"""

import argparse
import collections
import hashlib
import json
import pathlib
import re


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw", type=pathlib.Path)
    args = parser.parse_args()
    raw = args.raw.read_bytes()
    chunks = collections.defaultdict(list)
    actions = collections.defaultdict(list)
    for number, line in enumerate(raw.splitlines(), 1):
        try:
            event = json.loads(line)
        except (ValueError, UnicodeDecodeError) as error:
            raise SystemExit(f"invalid go-test JSON at line {number}: {error}")
        identity = (event.get("Package", ""), event.get("Test", ""))
        actions[identity].append(event.get("Action", ""))
        chunks[identity].append(event.get("Output", ""))

    parent = "TestPruneWholeCurrentPublicationSustainedPreflight"
    pattern = re.compile(
        r"WHOLE_PUBLIC_CURRENT rows=(\d+) pointer=(true|false) "
        r"writes=(\d+) ACK=(\d+) error=(.*?) last=(.*?) "
        r"requestCaptured=(true|false) nativeSelected=(\d+) "
        r"nativePrepared=(\d+) nativeCut=(\d+) nativePhase=(\d+)"
    )
    cells = []
    errors = []
    for identity, outputs in chunks.items():
        if not identity[1].startswith(parent + "/"):
            continue
        matches = list(pattern.finditer("".join(outputs)))
        if len(matches) != 1:
            errors.append(f"{identity[1]}: expected one result, found {len(matches)}")
            continue
        rows, pointer, writes, ack, error, stats, captured, selected, prepared, cut, phase = matches[0].groups()
        cell = {
            "test": identity[1], "rows": int(rows), "pointer": pointer == "true",
            "writes": int(writes), "ack": int(ack), "error": error,
            "request_captured": captured == "true", "native_selected": int(selected),
            "native_prepared": int(prepared), "native_cut": int(cut), "native_phase": int(phase),
        }
        checks = {
            "test_pass": "pass" in actions[identity] and "fail" not in actions[identity],
            "no_error": error == "<nil>", "once_ack": int(ack) == 1,
            "intervening_writes": int(writes) > 0, "request_captured": captured == "true",
            "native_selection": int(selected) > 0, "native_preparation": int(prepared) > 0,
            "native_current_cut": int(cut) > 0,
        }
        for name, maximum in [("WorkRecords", 32), ("WorkBytes", 1 << 20)]:
            count = re.search(r"\b" + name + r":(\d+)", stats)
            checks[name + "_reported_bounded"] = count is not None and 0 < int(count[1]) <= maximum
        cell["checks"] = checks
        cells.append(cell)
        errors.extend(f"{identity[1]}: {name}" for name, passed in checks.items() if not passed)

    expected = {(rows, pointer) for rows in (33, 80) for pointer in (False, True)}
    actual = [(cell["rows"], cell["pointer"]) for cell in cells]
    if len(actual) != 4 or set(actual) != expected:
        errors.append("expected exactly the original 33/80 x inline/pointer four cells")
    for identity, events in actions.items():
        if identity[1] in ("", parent) and ("pass" not in events or "fail" in events):
            errors.append(f"{identity}: parent/package did not pass")
    report = {
        "scope": "Public fixture activation only; original physical-Finish, budget, performance and final gates remain separate.",
        "raw_sha256": hashlib.sha256(raw).hexdigest(), "raw_bytes": len(raw),
        "cells": cells, "pass": not errors, "errors": errors,
    }
    print(json.dumps(report, indent=2))
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
