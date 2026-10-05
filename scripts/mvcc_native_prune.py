#!/usr/bin/env python3
"""Fresh-process native fixed-Q capture and fail-closed packet validation (stdlib only)."""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from unittest.mock import patch

BENCH = "TreeDB/mvcc/native_prune_bench_test.go"
HARNESS = [BENCH, "scripts/mvcc_native_prune.py"]
GAPS = ["storage_sync_count", "fence_wait_max_ns", "fence_hold_max_ns",
        "owned_retained_bytes", "owned_peak_bytes"]
CASES = [f"ordered/{order}/{n}" for order in ("shuffled", "ascending") for n in (512, 1024)] + [
    f"queued/{profile}/{n}" for profile in ("command_wal_durable", "no_wal_fast") for n in (512, 4096)]
SMOKE = ["smoke/ascending/8"]
COUNTERS = "Calls Records Bytes Visited SetupRecords CleanupRecords Deletes Batches MaxRecords MaxBytes AllocBytes Allocs".split()
TIMES = "SetupNS ColdOpenNS PassNS MaxQuantumNS ACKReopenNS CursorCloseNS CloseNS".split()


def check(ok, message):
    if not ok:
        raise ValueError(message)


def run(argv, cwd=None, env=None):
    return subprocess.check_output(argv, cwd=cwd, env=env, text=True, stderr=subprocess.DEVNULL).strip()


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def sources(root):
    # Include test sources: go test compiles them too. No per-record evidence log.
    return {str(p.relative_to(root)): digest(p) for p in sorted(root.rglob("*"))
            if p.is_file() and ".git" not in p.parts and
            (p.suffix in (".go", ".c", ".h", ".s", ".S", ".syso") or p.name in ("go.mod", "go.sum"))
            and str(p.relative_to(root)) != BENCH}


def build_env():
    env = os.environ.copy()
    env.pop("GOROOT", None)
    env.update(GOWORK="off", GOTOOLCHAIN="local", CGO_ENABLED="1", GOFLAGS="-p=2")
    return env


def identity(runtime, harness, scope, go, runtime_commit=None, archive=None):
    def commit(root):
        try:
            return run(["git", "rev-parse", "HEAD"], root)
        except subprocess.CalledProcessError:
            return None
    if scope == "full":
        for root in (runtime, harness):
            check(not run(["git", "status", "--porcelain", "--untracked-files=all"], root), "full capture requires clean committed sources")
        check(not archive and not runtime_commit, "full capture requires actual runtime commit")
    actual_commit = commit(runtime)
    check(actual_commit or (scope == "smoke" and runtime_commit and archive), "snapshot smoke requires base commit and frozen archive")
    return {"runtime_commit": actual_commit,
            "runtime_snapshot_base_commit": runtime_commit,
            "runtime_archive_sha256": digest(archive) if archive else None,
            "harness_commit": commit(harness), "runtime_files": sources(runtime),
            "harness_files": {name: digest(harness / name) for name in HARNESS},
            "go_version": run([go, "version"], env=build_env()),
            "go_env": json.loads(run([go, "env", "-json", "GOOS", "GOARCH", "CGO_ENABLED", "GOFLAGS", "GOTOOLCHAIN", "GOWORK"], env=build_env())),
            "scope": scope}


def validate(packet, expected, qualify=False):
    check(expected["scope"] in ("smoke", "full"), "invalid evidence scope")
    for key, size in (("runtime_commit", 40), ("harness_commit", 40),
                      ("runtime_snapshot_base_commit", 40), ("runtime_archive_sha256", 64)):
        check(key in expected, f"missing provenance field: {key}")
        value = expected[key]
        check(value is None or (isinstance(value, str) and re.fullmatch(r"[0-9a-f]{%d}" % size, value)), f"invalid provenance: {key}")
    base, archive = expected["runtime_snapshot_base_commit"], expected["runtime_archive_sha256"]
    check((base is None) == (archive is None), "snapshot base and archive must be paired")
    check(expected["runtime_commit"] is not None or (base and archive), "missing committed or frozen runtime provenance")
    for key in ("runtime_files", "harness_files"):
        check(isinstance(expected[key], dict) and expected[key], "missing source bindings")
        check(all(isinstance(name, str) and re.fullmatch(r"[0-9a-f]{64}", value or "") for name, value in expected[key].items()), "invalid source hash")
    check(set(expected["harness_files"]) == set(HARNESS), "missing harness bindings")
    if expected["scope"] == "full":
        check(all(re.fullmatch(r"[0-9a-f]{40}", expected[key] or "") for key in ("runtime_commit", "harness_commit")), "missing exact committed identities")
        check(expected["runtime_archive_sha256"] is None and expected["runtime_snapshot_base_commit"] is None, "full qualification cannot use a provisional snapshot")
    check(not packet.get("execution_error") and not packet.get("identity_after_error"), "capture failed; see retained packet errors")
    check(packet["schema"] == 1 and packet["identity_before"] == expected, "unexpected source/toolchain identity")
    check(packet["identity_after"] == expected, "source drift")
    check(packet["options"] == {"records": 32, "bytes": 1048576, "batch": 1, "mode": "CommitDurable", "iterations": 1, "tags": "mvcc_native_prune,treedb_test"}, "incorrect quantum/options")
    check(re.fullmatch(r"go version go1\.26\.4 \S+", expected["go_version"]) is not None, "unexpected Go toolchain")
    check(expected["go_env"]["CGO_ENABLED"] == "1" and expected["go_env"]["GOWORK"] == "off" and expected["go_env"]["GOTOOLCHAIN"] == "local", "unexpected build environment")
    rows = packet["results"]
    cases = SMOKE if expected["scope"] == "smoke" else CASES
    check(len(rows) == len(cases) and {r["case"] for r in rows} == set(cases), "missing/duplicate/unexpected subcases")
    check(type(packet["build_returncode"]) is int and packet["build_returncode"] == 0, "build failed or invalid exit status")
    for row in rows:
        name = row["case"]
        check(type(row["returncode"]) is int and row["returncode"] == 0 and row["Error"] == "", f"unclassified execution error: {name}")
        check(all(row[k] is True for k in ("Complete", "Oracle", "CursorClosed")), f"missing completion/oracle: {name}")
        check(type(row["FirstACKReopened"]) is bool, f"invalid recovery flag: {name}")
        for key in COUNTERS + TIMES:
            check(type(row[key]) is int and row[key] >= 0, f"invalid {key}: {name}")
        queued = name.startswith("queued/")
        history = 1024 if queued else int(name.rsplit("/", 1)[1])
        extra = int(name.rsplit("/", 1)[1]) if queued else 0
        profile = name.split("/")[1] if queued else "command_wal_durable"
        order = "pairwise" if queued else name.split("/")[1]
        check((row["history"], row["extra"], row["profile"], row["order"]) == (history, extra, profile, order), f"incorrect fixture/profile: {name}")
        want = history if queued else history - 1
        check(row["Deletes"] == row["Batches"] == want, f"incorrect ACK counts: {name}")
        check(0 < row["Calls"] <= (history + extra)*16, f"call cap: {name}")
        check(0 < row["MaxRecords"] <= 32 and 0 < row["MaxBytes"] <= 1048576, f"quantum cap: {name}")
        check(row["MaxRecords"] <= row["Records"] <= row["Calls"]*32 and row["MaxBytes"] <= row["Bytes"] <= row["Calls"]*1048576, f"counter reconciliation: {name}")
        check(row["SetupRecords"] + row["CleanupRecords"] <= row["Records"], f"setup/cleanup accounting: {name}")
        check(row["Visited"] <= row["Records"], f"uncharged visits: {name}")
        check(0 < row["MaxQuantumNS"] <= row["PassNS"] and row["SetupNS"] > 0 and row["CloseNS"] > 0, f"missing timings: {name}")
        check(row["AllocationScope"] == "combined_process_during_pass", "invalid allocation attribution")
        if queued:
            check(row["FirstACKReopened"] and row["ColdOpenNS"] > 0 and row["ACKReopenNS"] > 0, "missing first-ACK reopen")
        else:
            check(not row["FirstACKReopened"] and row["Records"] <= history*512, "original scaling cap")
    if expected["scope"] == "full":
        by_case = {r["case"]: r for r in rows}
        for order in ("shuffled", "ascending"):
            check(by_case[f"ordered/{order}/1024"]["Records"] <= 3*by_case[f"ordered/{order}/512"]["Records"], "original doubling cap")
    check(packet["missing_runtime_measurements"] == GAPS, "runtime measurement gaps must be explicit")
    check(packet["verdict"] == "tooling_only", "unsupported qualification verdict")
    # This schema revision has no owner-boundary measurement input. Adding it
    # requires reviewed runtime instrumentation and a reviewed schema revision.
    check(not qualify, "qualification refused: " + ", ".join(GAPS))
    return "valid tooling packet; native qualification unavailable"


def capture(args):
    runtime, harness = Path(args.runtime).resolve(), Path(args.harness).resolve()
    out = Path(args.out).resolve()
    check(runtime not in out.parents and harness not in out.parents, "output must be outside runtime/harness sources")
    check(not out.exists(), "output already exists; preserve earlier evidence")
    out.mkdir(parents=True)
    expected = json.loads(Path(args.expected).read_text())
    env = build_env()
    packet = {"schema": 1, "identity_before": None, "identity_after": None,
              "options": {"records": 32, "bytes": 1048576, "batch": 1, "mode": "CommitDurable", "iterations": 1, "tags": "mvcc_native_prune,treedb_test"},
              "build_returncode": None, "results": [], "missing_runtime_measurements": GAPS, "verdict": "tooling_only"}
    try:
        before = identity(runtime, harness, expected["scope"], args.go, args.runtime_commit, args.archive)
        packet["identity_before"] = before
        check(before == expected, "unexpected source identity before build")
        overlay = out / "overlay.json"
        overlay.write_text(json.dumps({"Replace": {str(runtime / BENCH): str(harness / BENCH)}}))
        binary = out / "mvcc.test"
        command = [args.go, "test", "-c", "-tags=mvcc_native_prune,treedb_test", "-overlay=" + str(overlay), "-o", str(binary), "./TreeDB/mvcc"]
        (out / "build-command.json").write_text(json.dumps(command))
        with (out / "build.log").open("w") as log:
            packet["build_returncode"] = subprocess.run(command, cwd=runtime, env=env, stdout=log, stderr=subprocess.STDOUT).returncode
        check(packet["build_returncode"] == 0, "tagged API build failed; see retained build.log")
        for index, case in enumerate(SMOKE if expected["scope"] == "smoke" else CASES):
            result = out / f"case-{index}.json"
            case_env = dict(env, MVCC_NATIVE_CASE=case, MVCC_NATIVE_RESULT=str(result), MVCC_NATIVE_SMOKE="1" if expected["scope"] == "smoke" else "0")
            command = [str(binary), "-test.run=^$", "-test.bench=^BenchmarkNativePruneFixedQ$", "-test.benchtime=1x", "-test.count=1", "-test.timeout=120s"]
            if args.profiles:
                command += [f"-test.cpuprofile={out}/case-{index}.cpu.pprof", f"-test.memprofile={out}/case-{index}.allocs.pprof", "-test.memprofilerate=1", f"-test.mutexprofile={out}/case-{index}.mutex.pprof", "-test.mutexprofilefraction=1", f"-test.blockprofile={out}/case-{index}.block.pprof", "-test.blockprofilerate=1"]
            (out / f"case-{index}-command.json").write_text(json.dumps(command))
            with (out / f"case-{index}.log").open("w") as log:
                code = subprocess.run(command, cwd=out, env=case_env, stdout=log, stderr=subprocess.STDOUT).returncode
            row = json.loads(result.read_text()) if result.exists() else {"case": case, "Error": "missing result"}
            row["returncode"] = code
            packet["results"].append(row)
            check(code == 0, f"subcase failed: {case}; failed evidence preserved")
    except Exception as error:
        packet["execution_error"] = str(error)
        raise
    finally:
        try:
            packet["identity_after"] = identity(runtime, harness, expected["scope"], args.go, args.runtime_commit, args.archive)
        except Exception as error:
            packet["identity_after_error"] = str(error)
        (out / "packet.json").write_text(json.dumps(packet, indent=2) + "\n")
    print(validate(packet, expected))


def self_test():
    ident = {"runtime_commit": None, "harness_commit": None, "runtime_snapshot_base_commit": "c"*40, "runtime_archive_sha256": "d"*64, "runtime_files": {"TreeDB/mvcc/versions.go": "a"*64}, "harness_files": {name: "b"*64 for name in HARNESS}, "scope": "smoke", "go_version": "go version go1.26.4 linux/amd64", "go_env": {"CGO_ENABLED": "1", "GOWORK": "off", "GOTOOLCHAIN": "local"}}
    row = dict.fromkeys(COUNTERS + TIMES, 1)
    row.update(case=SMOKE[0], profile="command_wal_durable", order="ascending", history=8, extra=0, Calls=8, Records=8, Bytes=8, Deletes=7, Batches=7, PassNS=2, SetupRecords=0, CleanupRecords=0, Complete=True, Oracle=True, CursorClosed=True, FirstACKReopened=False, AllocationScope="combined_process_during_pass", Error="", returncode=0)
    packet = dict(schema=1, identity_before=ident, identity_after=ident, options={"records":32,"bytes":1048576,"batch":1,"mode":"CommitDurable","iterations":1,"tags":"mvcc_native_prune,treedb_test"}, build_returncode=0, results=[row], missing_runtime_measurements=GAPS, verdict="tooling_only")
    validate(packet, ident)
    bad = []
    for key, value in (("results", []), ("verdict", "qualified"), ("missing_runtime_measurements", []), ("identity_after", {})):
        p = copy.deepcopy(packet); p[key] = value; bad.append(p)
    for key, value in (("Complete", False), ("Oracle", False), ("MaxRecords", 33), ("Records", -1), ("Visited", 2**60), ("FirstACKReopened", 0), ("returncode", False), ("Error", "unknown"), ("profile", "wrong")):
        p = copy.deepcopy(packet); p["results"][0][key] = value; bad.append(p)
    for p in bad:
        try: validate(p, ident)
        except (ValueError, KeyError): continue
        raise AssertionError("corrupt packet accepted")
    for key, value in (("runtime_archive_sha256", None), ("runtime_snapshot_base_commit", "invalid"), ("runtime_commit", False)):
        wrong = copy.deepcopy(ident); wrong[key] = value
        try: validate(packet, wrong)
        except ValueError: continue
        raise AssertionError("invalid provenance accepted")
    missing = copy.deepcopy(ident); del missing["harness_commit"]
    try: validate(packet, missing)
    except ValueError: pass
    else: raise AssertionError("missing provenance accepted")
    wrong = copy.deepcopy(packet); wrong["build_returncode"] = False
    try: validate(wrong, ident)
    except ValueError: pass
    else: raise AssertionError("boolean exit status accepted")
    # No real build: preserve both errors when final source inspection fails.
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory); expected = root / "expected.json"
        expected.write_text(json.dumps(ident))
        args = argparse.Namespace(runtime=str(root / "runtime"), harness=str(root / "harness"), out=str(root / "capture"), expected=str(expected), go="unused", runtime_commit=None, archive=None, profiles=False)
        with patch(__name__ + ".identity", side_effect=[ident, ValueError("dirty final sources")]), patch("subprocess.run", return_value=subprocess.CompletedProcess([], 1)):
            try: capture(args)
            except ValueError as error: check("tagged API build failed" in str(error), "original error lost")
            else: raise AssertionError("failed build accepted")
        failed = json.loads((root / "capture/packet.json").read_text())
        check("tagged API build failed" in failed["execution_error"] and failed["identity_after_error"] == "dirty final sources", "failure packet lost errors")
    try: validate(packet, ident, qualify=True)
    except ValueError: pass
    else: raise AssertionError("missing runtime measurements qualified")
    print("self-test: valid smoke accepted; malformed/missing/drifted packets and qualification refused")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("freeze", "capture"):
        p = sub.add_parser(name)
        for arg in ("runtime", "harness", "out", "go"): p.add_argument("--" + arg, required=True)
        p.add_argument("--runtime-commit"); p.add_argument("--archive")
        if name == "freeze": p.add_argument("--scope", choices=("smoke", "full"), required=True)
        else:
            p.add_argument("--expected", required=True); p.add_argument("--profiles", action="store_true")
    p = sub.add_parser("validate"); p.add_argument("packet"); p.add_argument("--expected", required=True); p.add_argument("--qualify", action="store_true")
    sub.add_parser("self-test")
    args = parser.parse_args()
    if args.command == "self-test": self_test()
    elif args.command == "validate": print(validate(json.loads(Path(args.packet).read_text()), json.loads(Path(args.expected).read_text()), args.qualify))
    elif args.command == "capture": capture(args)
    else:
        check(not Path(args.out).exists(), "expected identity already exists")
        Path(args.out).write_text(json.dumps(identity(Path(args.runtime).resolve(), Path(args.harness).resolve(), args.scope, args.go, args.runtime_commit, args.archive), indent=2) + "\n")


if __name__ == "__main__":
    try: main()
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as error:
        print(f"REFUSED: {error}", file=sys.stderr); sys.exit(1)
