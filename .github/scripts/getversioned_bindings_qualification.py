#!/usr/bin/env python3
"""Fixed #5396 event inventory and qualification-output ledger; no Go execution."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys

LOG_LIMIT = 32 * 1024 * 1024
OUTPUT_LIMIT = 1024 * 1024 * 1024
DB = "github.com/snissn/gomap/TreeDB/db"
TREE = "github.com/snissn/gomap/TreeDB/tree"
DB_TOPS = ('TestNegativeFilterIndexReplacementOldSnapshotAndGuardRelease', 'TestOneShotAppendBindingsRetainMutablePolicy', 'TestOneShotAppendBindingsUseCurrentOwner', 'TestOneShotEmptyReadRetainsOnlyLegacyStateWrapper', 'TestOneShotLiveValueLogReadSurvivesConcurrentDBClose', 'TestOneShotReadAllocationBudget', 'TestOneShotReadCapturePublicationAndRelease', 'TestOneShotReadCustodyMatchesCapturedResources', 'TestOneShotReadOwnedMissingEmptyAndNilKey', 'TestOneShotReadRejectedCaptureReleasesValueLogPin', 'TestOwnedBackendSuccessorCapturedRootLeafPointerAndReuse', 'TestOwnedBackendSuccessorPrimaryAndConcurrentCaptures', 'TestSnapshotAllocationAdmissionRefusesRefreshedExactView', 'TestSnapshotCleanupCompletionDoesNotWaitForIndependentPagerDescriptors', 'TestSnapshotCleanupCompletionJoinsRunningAndCompletedOriginal', 'TestSnapshotCleanupCompletionReportsActualPagerLastPhysicalError', 'TestSnapshotCleanupCompletionSharesOriginalFinalReader')
TREE_TOPS = ('TestTreeAppendReadBindingsMatchReset', 'TestTreeGenericResetRetainsDynamicKeyPolicy', 'TestTreeGetAppend_AppendsAndUsesAppendReaderForPointers', 'TestTreeGetAppend_LeafRefChecksumPolicyHonored', 'TestTreeGetAppend_LeafRefPrefersReadUnsafeTo', 'TestTreeGetAppend_UsesAppendReaderForLeafRefPages', 'TestTreeGetAppend_UsesLeafLogPageReaderForLeafRefPages', 'TestTreeGetUnsafe_UsesUnsafeReaderForPointers', 'TestTreeGet_UsesAppendReaderForPointersAndReturnsSafeCopy', 'TestTreeLeafLogUnverifiedStateStillChecksChecksum', 'TestTreeLeafLogVerifiedCacheStateSkipsChecksum', 'TestTreePageLimitResetClearsSelectedExtent')
TOPS = {DB: set(DB_TOPS), TREE: set(TREE_TOPS)}
NEW = {
    DB: {"TestOneShotAppendBindingsUseCurrentOwner", "TestOneShotAppendBindingsRetainMutablePolicy"},
    TREE: {"TestTreeAppendReadBindingsMatchReset", "TestTreeAppendReadBindingsMatchReset/first",
           "TestTreeAppendReadBindingsMatchReset/second", "TestTreeAppendReadBindingsMatchReset/nil",
           "TestTreeGenericResetRetainsDynamicKeyPolicy", "TestTreeGenericResetRetainsDynamicKeyPolicy/enabled",
           "TestTreeGenericResetRetainsDynamicKeyPolicy/disabled"},
}
NEW_TOPS = {n.split("/")[0] for names in NEW.values() for n in names}

# Exact source scope for compiler diagnostics, not a verdict about escapes or
# machine calls. All unknown/error text survives selection. The ordinary
# optimized ELFs and their nm/objdump captures remain the structural evidence.
COMPILER_SOURCES = {
    "db": {
        "TreeDB/db/value_reader.go": ("5e91f5d3f97f1a85f22a2f7995bb9663a75030c1f8d00ef64722ab7763d7db17", ((63, 75),)),
        "TreeDB/db/db.go": ("53f43cd2ccd5611eae203b19046900c3f1e486d6dc0d65134f0b7857c0ae64d1", ((1787, 1968),)),
    },
    "tree": {
        "TreeDB/tree/tree.go": ("94acd5911f9c8fce15f3ac382a36c36f6ef0202c665d946e70914e6aa720e411", ((369, 487),)),
    },
}
POSITION = re.compile(r"^(.*\.go):(\d+):(\d+): ?(.*)$")
# Go1.26.10 inline/inl.go and escape/{escape,solve,graph}.go emit these
# optimization-only forms. In particular the runtime prohibition is an actual
# compiler error, and warnings/unrecognized messages are never omitted.
OPTIMIZATION = re.compile(
    r"^(?:cannot inline .+|can inline .+|inlining call to .+|"
    r".+ capturing by (?:value|ref): .+|moved to heap: .+|"
    r".+ escapes to heap(?: in .+:)?|.+ does not escape|"
    r"parameter .+ leaks to .+ for .+ with derefs=\d+:|"
    r"leaking param(?: content)?: .+|zero-copy string->\[\]byte conversion|"
    r"  flow: .+|    from .+)$"
)


def compiler_scope(arm, repository):
    sources = COMPILER_SOURCES[arm]
    for name, (expected, _) in sources.items():
        if hashlib.sha256((repository / name).read_bytes()).hexdigest() != expected:
            raise ValueError("compiler selection source identity changed")
    return sources


def omit_optimization(line, sources):
    try:
        match = POSITION.fullmatch(line.decode("utf-8").rstrip("\r\n"))
    except UnicodeDecodeError:
        return False
    if not match:
        return False
    path, number, _, message = match.groups()
    if "not allowed in runtime" in message or not OPTIMIZATION.fullmatch(message):
        return False
    path = path.replace("\\", "/")
    for name, (_, ranges) in sources.items():
        if path.rsplit("/", 1)[-1] == Path(name).name:
            if any(first <= int(number) <= last for first, last in ranges):
                return False
    return True


def compiler_stream(arm, root, incoming, repository):
    """Consume the entire stderr stream, selecting only named optimizer scope.

    Raw bytes are hashed/counted, never represented as an uploaded raw log.
    Incomplete input or any limit/error refuses qualification. Bash separately
    retains the original compiler/timeout exit, so selection cannot hide it.
    """
    sources = compiler_scope(arm, repository)
    bounds(root)
    record = {"schema": "getversioned-compiler-selection-v1", "arm": arm,
              "scope": {n: {"sha256": h, "inclusive_lines": spans} for n, (h, spans) in sources.items()},
              "status": "INCOMPLETE", "raw_eof": False, "raw_bytes": 0,
              "raw_lines": 0, "retained_bytes": 0, "retained_lines": 0,
              "omitted_optimization_bytes": 0, "omitted_optimization_lines": 0,
              "raw_uploaded": False, "compiler_exit_required_separately": True}
    digest = hashlib.sha256()
    try:
        with (root / (arm + "-compiler.selected.stderr")).open("xb") as selected:
            while True:
                line = incoming.readline(LOG_LIMIT + 1)
                if not line:
                    record["raw_eof"] = True
                    record["status"] = "SCOPED_STREAM_COMPLETE_REVIEW_PENDING"
                    break
                digest.update(line)
                record["raw_bytes"] += len(line)
                record["raw_lines"] += 1
                if len(line) > LOG_LIMIT:
                    raise ValueError("compiler diagnostic line exceeds32MiB")
                if omit_optimization(line, sources):
                    record["omitted_optimization_bytes"] += len(line)
                    record["omitted_optimization_lines"] += 1
                else:
                    if record["retained_bytes"] + len(line) > LOG_LIMIT:
                        raise ValueError("selected compiler text exceeds32MiB")
                    selected.write(line)
                    record["retained_bytes"] += len(line)
                    record["retained_lines"] += 1
    finally:
        record["raw_sha256" if record["raw_eof"] else "consumed_prefix_sha256"] = digest.hexdigest()
        with (root / (arm + "-compiler-stream.json")).open("x", encoding="utf-8") as receipt:
            json.dump(record, receipt, indent=2, sort_keys=True)
            receipt.write("\n")
    bounds(root)
    return record


def events(path):
    if path.stat().st_size > LOG_LIMIT:
        raise ValueError("event log exceeds32MiB")
    ran, passed, packages = set(), set(), set()
    build_paths = {p + suffix for p in TOPS for suffix in (" [" + p + ".test]", "_test [" + p + ".test]", ".test")}
    for line in path.open(encoding="utf-8"):
        e = json.loads(line)
        if not isinstance(e, dict) or not isinstance(e.get("Action"), str):
            raise ValueError("malformed Go event")
        action = e["Action"]
        if action == "build-output":
            if e.get("ImportPath") not in build_paths or "Package" in e or not isinstance(e.get("Output"), str):
                raise ValueError("foreign compiler event")
            continue
        pkg = e.get("Package")
        if pkg not in TOPS or action not in {"start", "run", "pause", "cont", "output", "pass", "fail", "skip", "bench"}:
            raise ValueError("foreign or unsupported Go event")
        if action in {"fail", "skip", "bench"} or "WARNING: DATA RACE" in e.get("Output", ""):
            raise ValueError("adverse Go event")
        name = e.get("Test")
        if name is None:
            if action == "pass":
                if pkg in packages:
                    raise ValueError("duplicate package PASS")
                packages.add(pkg)
            elif action not in {"start", "output"}:
                raise ValueError("unexpected package action")
            continue
        if not isinstance(name, str) or name.split("/")[0] not in TOPS[pkg] or pkg in packages:
            raise ValueError("foreign or late test event")
        if name.split("/")[0] in NEW_TOPS and name not in NEW[pkg]:
            raise ValueError("unexpected new-test descendant")
        key = (pkg, name)
        if action == "run":
            if key in ran:
                raise ValueError("duplicate test RUN")
            ran.add(key)
        elif action == "pass":
            if key not in ran or key in passed:
                raise ValueError("unentered or duplicate test PASS")
            passed.add(key)
        elif action not in {"output", "pause", "cont"}:
            raise ValueError("unexpected test action")
    required = {(p, n) for p, names in TOPS.items() for n in names} | {(p, n) for p, names in NEW.items() for n in names}
    if not required <= passed or ran != passed or packages != set(TOPS):
        raise ValueError("missing or unfinished positive inventory")
    return {"status": "EXACT_FOCUSED_INVENTORY_PASS", "tops": 29,
            "new_outcomes": 9, "new_leaves": 7, "observed_outcomes": len(passed),
            "normal_allocation_only": True}


def output_files(root):
    expected = Path(os.environ["RUNNER_TEMP"]).resolve() / "getversioned-bindings-qualification"
    if root.resolve() != expected or root.is_symlink() or not root.is_dir():
        raise ValueError("foreign output directory")
    rows = []
    for p in sorted(root.iterdir()):
        st = p.lstat()
        if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
            raise ValueError("unexpected output kind")
        rows.append((p, st.st_size))
    return rows


def bounds(root):
    rows = output_files(root)
    total = sum(size for _, size in rows)
    logs = [(p.name, size) for p, size in rows if p.suffix != ".test"]
    if total > OUTPUT_LIMIT or any(size > LOG_LIMIT for _, size in logs):
        raise ValueError("qualification output bound exceeded")
    return {"bytes": total, "scope": "qualification directory only; boundary checks, no live RSS guarantee"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("selector", "normal", "race", "compiler-db", "compiler-tree", "bounds", "manifest"))
    parser.add_argument("directory", nargs="?", type=Path)
    a = parser.parse_args()
    if a.mode == "selector":
        print("^(" + "|".join(sorted(DB_TOPS + TREE_TOPS)) + ")$")
        return
    if a.directory is None:
        parser.error("directory required")
    root = a.directory
    if a.mode.startswith("compiler-"):
        result = compiler_stream(a.mode.removeprefix("compiler-"), root, sys.stdin.buffer, Path(__file__).resolve().parents[2])
        print(json.dumps(result, sort_keys=True))
        return
    if a.mode in {"normal", "race"}:
        bounds(root)
        result = events(root / (a.mode + ".jsonl"))
        result["mode"] = a.mode
        result["allocation_qualified_by_race"] = False
        print(json.dumps(result, sort_keys=True))
        return
    if a.mode == "bounds":
        print(json.dumps(bounds(root), sort_keys=True))
        return
    # Always-run final ledger: persist current failure before reporting it.
    rows = output_files(root)
    record = {"schema": "getversioned-qualification-output-v1", "files": {}, "bounds": "UNVERIFIED"}
    for p, size in rows:
        h = hashlib.sha256()
        with p.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        record["files"][p.name] = {"bytes": size, "sha256": h.hexdigest()}
    try:
        record["bounds"] = bounds(root)
    except ValueError as e:
        record["bounds"] = str(e)
    payload = (json.dumps(record, indent=2, sort_keys=True) + "\n").encode()
    if sum(size for _, size in rows) + len(payload) > OUTPUT_LIMIT:
        raise ValueError("no capacity for final ledger; existing raw outputs retained")
    target = root / "artifact-manifest.json"
    with target.open("xb") as f:
        f.write(payload)
    bounds(root)
    if isinstance(record["bounds"], str):
        raise ValueError(record["bounds"])
    print("output ledger retained; compiled structural/escape verdict requires exact-output review")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError) as e:
        print("qualification consumer refused: " + str(e), file=sys.stderr)
        sys.exit(1)
