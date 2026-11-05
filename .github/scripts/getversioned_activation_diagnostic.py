#!/usr/bin/env python3
"""Fixed source overlay and same-executable structural evidence checks.

The output ledger is a post-run output-directory check, not a live disk, RSS,
or host-wide limit. This helper never runs Go or the test executable.
"""

import argparse
import difflib
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
PINS = {
    "TreeDB/db/db.go": "53f43cd2ccd5611eae203b19046900c3f1e486d6dc0d65134f0b7857c0ae64d1",
    "TreeDB/db/pools.go": "1d791b040aa3b45fa9d2e8890d91b56ee81c1c0a4adda77cd72af5d3bdc081f7",
    "TreeDB/tree/tree.go": "94acd5911f9c8fce15f3ac382a36c36f6ef0202c665d946e70914e6aa720e411",
    "TreeDB/db/value_reader.go": "5e91f5d3f97f1a85f22a2f7995bb9663a75030c1f8d00ef64722ab7763d7db17",
}
ACQUIRE = "func (db *DB) acquireOneShotReadOrErr() (*oneShotRead, error) {\n"
BOUND = "\t\t\t\tsnap.tree.ResetAppendReadBindings(idx.pager, &bindings, state.RootPageID)\n"
BRIDGE = """
// Diagnostic-only bridge, present exclusively in the stock Go overlay.
var getVersionedDiagnosticAcquire atomic.Uint64
var getVersionedDiagnosticBoundReset atomic.Uint64

func ResetGetVersionedActivationDiagnostic() {
\tgetVersionedDiagnosticAcquire.Store(0)
\tgetVersionedDiagnosticBoundReset.Store(0)
}

func ReadGetVersionedActivationDiagnostic() (uint64, uint64) {
\treturn getVersionedDiagnosticAcquire.Load(), getVersionedDiagnosticBoundReset.Load()
}
"""
MAX_TEXT = 32 * 1024 * 1024
MAX_OUTPUT = 1024 * 1024 * 1024
CALL = re.compile(r"(?m)^[ \t]*\S+\.go:\d+[ \t]+0x[0-9a-fA-F]+[ \t]+[0-9a-fA-F]+[ \t]+CALL[ \t]+(\S+)(?:[ \t].*)?$")
CAPTURE_SYMBOL = "github.com/snissn/gomap/TreeDB/db.(*DB).captureSnapshotForReadWithValueLogPublicationLockHeld"
BOUND_SYMBOL = "github.com/snissn/gomap/TreeDB/tree.(*Tree).ResetAppendReadBindings"
GENERIC_SYMBOL = "github.com/snissn/gomap/TreeDB/tree.(*Tree).Reset"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def pinned_source(data, expected):
    if digest(data) != expected:
        raise ValueError("source hash mismatch")
    return data.decode("utf-8")


def unique_replace(source, anchor, replacement):
    if source.count(anchor) != 1:
        raise ValueError("source anchor missing or nonunique")
    return source.replace(anchor, replacement, 1)


def prepare(out):
    sources = {}
    for name, expected in PINS.items():
        data = (ROOT / name).read_bytes()
        try:
            sources[name] = pinned_source(data, expected)
        except ValueError as exc:
            raise ValueError("source hash mismatch: " + name) from exc
    db = unique_replace(sources["TreeDB/db/db.go"], BOUND,
                        BOUND + "\t\t\t\tgetVersionedDiagnosticBoundReset.Add(1)\n") + BRIDGE
    pools = unique_replace(sources["TreeDB/db/pools.go"], ACQUIRE,
                           ACQUIRE + "\tgetVersionedDiagnosticAcquire.Add(1)\n")
    out.mkdir(parents=True, exist_ok=True)
    replacements = {}
    records = {}
    for name, content, leaf in [("TreeDB/db/db.go", db, "db.go.overlay"),
                                ("TreeDB/db/pools.go", pools, "pools.go.overlay")]:
        target = out / leaf
        target.write_text(content, encoding="utf-8")
        replacements[str((ROOT / name).resolve())] = str(target.resolve())
        records[name] = {"original_sha256": PINS[name], "overlay_sha256": digest(content.encode())}
        diff = difflib.unified_diff(sources[name].splitlines(True), content.splitlines(True),
                                    fromfile=name, tofile=leaf)
        (out / (leaf + ".diff")).write_text("".join(diff), encoding="utf-8")
    (out / "overlay.json").write_text(json.dumps({"Replace": replacements}, indent=2) + "\n", encoding="utf-8")
    (out / "overlay-source.json").write_text(json.dumps(records, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def structural_text(capture, bound, generic):
    if any(not text.strip() or len(text.encode()) > MAX_TEXT for text in (capture, bound, generic)):
        raise ValueError("missing or oversized objdump text")
    for text, symbol in ((capture, CAPTURE_SYMBOL), (bound, BOUND_SYMBOL), (generic, GENERIC_SYMBOL)):
        headers = re.findall(r"(?m)^TEXT (\S+)\(SB\)(?:\s+.*)?$", text)
        if headers != [symbol]:
            raise ValueError("wrong or missing objdump symbol: " + symbol)
    capture_calls = CALL.findall(capture)
    bound_calls = CALL.findall(bound)
    generic_calls = CALL.findall(generic)
    if BOUND_SYMBOL + "(SB)" not in capture_calls:
        raise ValueError("capture lacks bound-reset CALL")
    if any(re.search(r"(?:^|/)runtime\.(?:typeAssert|assertE2I|assertI2I|panicdottypeE|panicdottypeI|panicnildottype)", c) for c in bound_calls):
        raise ValueError("bound reset calls runtime interface assertion")
    return {"capture_calls": capture_calls, "bound_calls": bound_calls,
            "generic_calls": generic_calls, "interpretation": "direct CALL evidence only; no indirect-call, escape or equivalence claim"}


def structural(out):
    texts = []
    for name in ("capture.objdump.txt", "bound.objdump.txt", "generic.objdump.txt"):
        with (out / name).open("rb") as stream:
            data = stream.read(MAX_TEXT + 1)
        if len(data) > MAX_TEXT:
            raise ValueError("oversized objdump text: " + name)
        texts.append(data.decode("utf-8"))
    result = structural_text(*texts)
    (out / "structural.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))


def finish(out):
    out.mkdir(parents=True, exist_ok=True)
    rows, total, errors = [], 0, []
    for path in sorted(out.iterdir()):
        if path.name == "output-ledger.json":
            continue
        if path.is_symlink() or not path.is_file():
            errors.append("nonregular output: " + path.name)
            continue
        try:
            size = path.stat().st_size
            total += size
            if path.name != "db.test" and size > MAX_TEXT:
                errors.append("text exceeds bound: " + path.name)
            h = hashlib.sha256()
            with path.open("rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    h.update(block)
            rows.append({"name": path.name, "bytes": size, "sha256": h.hexdigest()})
        except OSError as exc:
            errors.append("cannot read output: " + path.name + ": " + type(exc).__name__)
    result = {"files": rows, "bytes": total, "errors": errors,
              "scope": "post-run output directory; ledger excluded from file hashes but included in ceiling"}
    payload = ledger_payload(result, total)
    (out / "output-ledger.json").write_bytes(payload)
    print(json.dumps(result, sort_keys=True))
    if errors:
        raise ValueError("output ledger bounds failed")


def ledger_payload(result, other_bytes):
    payload = (json.dumps(result, indent=2, sort_keys=True) + "\n").encode()
    if other_bytes + len(payload) > MAX_OUTPUT:
        result["errors"].append("output directory including ledger exceeds bound")
        payload = (json.dumps(result, indent=2, sort_keys=True) + "\n").encode()
    return payload


def inventory_text(text):
    names = {"TestGetVersionedWriteActivationDiagnostic",
             "TestGetVersionedWriteActivationDiagnostic/durable_write_window",
             "TestGetVersionedWriteActivationDiagnostic/fresh_backend_positive"}
    counts = {name: {"run": 0, "pass": 0} for name in names}
    package = "github.com/snissn/gomap/TreeDB"
    package_pass = 0
    for line in text.splitlines():
        event = json.loads(line)
        action, name = event.get("Action"), event.get("Test")
        output = event.get("Output", "")
        if "WARNING: DATA RACE" in output or action in ("fail", "skip", "bench"):
            raise ValueError("failed, skipped, benchmark, or race event")
        if action == "build-output" and not name:
            continue
        if event.get("Package") != package:
            raise ValueError("unexpected package")
        if name:
            if name not in names:
                raise ValueError("unexpected test: " + name)
            if action in ("run", "pass"):
                counts[name][action] += 1
                if counts[name][action] > 1 or (action == "pass" and counts[name]["run"] != 1):
                    raise ValueError("duplicate or unordered test event")
        elif action == "pass":
            package_pass += 1
    if package_pass != 1 or any(v != {"run": 1, "pass": 1} for v in counts.values()):
        raise ValueError("incomplete inventory")
    return {"package": package, "tests": sorted(names), "package_pass": package_pass}


def inventory(out):
    result = {}
    for mode in ("normal", "race"):
        with (out / (mode + ".jsonl")).open("rb") as stream:
            data = stream.read(MAX_TEXT + 1)
        if len(data) > MAX_TEXT:
            raise ValueError("oversized inventory")
        result[mode] = inventory_text(data.decode("utf-8"))
    (out / "inventory.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))


def selftest():
    sample = {"errors": []}
    initial_size = len((json.dumps(sample, indent=2, sort_keys=True) + "\n").encode())
    ledger_payload(sample, MAX_OUTPUT - initial_size)
    if sample["errors"]:
        raise AssertionError("exact ledger boundary rejected")
    sample = {"errors": []}
    ledger_payload(sample, MAX_OUTPUT - initial_size + 1)
    if not sample["errors"]:
        raise AssertionError("ledger boundary overflow accepted")
    try:
        pinned_source(b"foreign", digest(b"expected"))
    except ValueError:
        pass
    else:
        raise AssertionError("foreign source accepted")
    capture = "TEXT " + CAPTURE_SYMBOL + "(SB) /src/db.go\n  db.go:12 0x1234 e800000000 CALL " + BOUND_SYMBOL + "(SB)\n"
    bound = "TEXT " + BOUND_SYMBOL + "(SB) /src/tree.go\n  tree.go:13 0x1239 e800000000 CALL " + GENERIC_SYMBOL + "(SB)\n"
    generic = "TEXT " + GENERIC_SYMBOL + "(SB) /src/tree.go\n  tree.go:14 0x1240 c3 RET\n"
    structural_text(capture, bound, generic)
    for args in [(capture.replace("ResetAppendReadBindings(SB)", "NotResetAppendReadBindings(SB)"), bound, generic),
                 (capture.replace("  db.go:12", "# db.go:12"), bound, generic),
                 (capture, bound + "  tree.go:15 0x1241 e800000000 CALL runtime.typeAssert(SB)\n", generic),
                 (capture, bound + "  tree.go:15 0x1241 e800000000 CALL runtime.panicdottypeE(SB)\n", generic),
                 (capture.replace(CAPTURE_SYMBOL, CAPTURE_SYMBOL + "Wrong"), bound, generic),
                 (capture, bound, "nonempty")]:
        try:
            structural_text(*args)
        except ValueError:
            continue
        raise AssertionError("negative structural case accepted")
    for source, anchor in [("foreign", ACQUIRE), (ACQUIRE + ACQUIRE, ACQUIRE)]:
        try:
            unique_replace(source, anchor, "x")
        except ValueError:
            continue
        raise AssertionError("negative anchor accepted")
    package = "github.com/snissn/gomap/TreeDB"
    top = "TestGetVersionedWriteActivationDiagnostic"
    events = [{"Action": a, "Package": package, "Test": n} for n in
              (top, top + "/durable_write_window", top + "/fresh_backend_positive")
              for a in ("run", "pass")]
    events += [{"Action": "pass", "Package": package}]
    encode = lambda items: "\n".join(json.dumps(item) for item in items)
    inventory_text(encode(events))
    for bad in (events[:-1], events + [events[0]], events + [{"Action": "skip", "Package": package}],
                events + [{"Action": "output", "Package": package, "Output": "WARNING: DATA RACE"}],
                events[:2] + events[4:]):
        try:
            inventory_text(encode(bad))
        except ValueError:
            continue
        raise AssertionError("negative inventory accepted")
    print("fixed controls PASS")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("prepare", "structural", "inventory", "finish", "selftest"))
    parser.add_argument("output", nargs="?")
    args = parser.parse_args()
    if args.mode == "selftest":
        selftest()
        return
    if not args.output:
        parser.error("output directory required")
    out = Path(args.output)
    {"prepare": prepare, "structural": structural, "inventory": inventory, "finish": finish}[args.mode](out)


if __name__ == "__main__":
    main()
