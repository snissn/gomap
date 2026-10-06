#!/usr/bin/env python3
"""Branch-local Windows diagnosis; original sources first, no retries/product gates."""
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import time

PINS = [
    ("base", "7649857532521db69ff4cdcf236a11377c85ab3d"),
    ("common-A", "dca8ab478bae64e47ca92aeddaf0b2fe4ddffa39"),
    ("R", "363d8593d6c8e9d23a49d59c33c97a85ba087f77"),
]
CONTEXT_PATH = Path(__file__).with_name("windows_handle_full_context.json")
CONTEXT = json.loads(CONTEXT_PATH.read_text(encoding="utf-8"))
TEST_NAMES = CONTEXT["test_order"]
COMMAND = ["go", "test", "-json", "-timeout", "30m", "-p", "1", ".", "-run", "^(" + "|".join(TEST_NAMES) + ")$", "-count=1"]
REPO = Path.cwd()
OUT = Path(sys.argv[1]).resolve()
SOURCES = Path(os.environ["RUNNER_TEMP"]) / "windows-handle-private-sources"


def capture(command, cwd=REPO):
    return subprocess.check_output(command, cwd=cwd).decode("utf-8", errors="replace").strip()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def checkout(label, sha):
    target = SOURCES / label
    subprocess.run(["git", "cat-file", "-e", sha + "^{commit}"], cwd=REPO, check=True)
    subprocess.run(["git", "-c", "core.autocrlf=false", "worktree", "add", "--detach", str(target), sha], cwd=REPO, check=True)
    assert capture(["git", "rev-parse", "HEAD"], target) == sha
    assert not capture(["git", "status", "--porcelain"], target)
    return target


def source_manifest(source, sha, destination, allow_overlay=False):
    entries = subprocess.check_output(["git", "ls-tree", "-rz", "--full-tree", sha], cwd=REPO).split(b"\0")
    records = []
    mismatches = []
    for entry in entries:
        if not entry:
            continue
        meta, raw_path = entry.split(b"\t", 1)
        mode, kind, blob = meta.decode().split()
        path = raw_path.decode("utf-8")
        if kind != "blob":
            raise RuntimeError("unexpected non-blob source entry: " + path)
        data = (source / path).read_bytes()
        actual = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
        match = actual == blob
        records.append({"path": path, "mode": mode, "git_blob": blob, "actual_blob": actual, "sha256": hashlib.sha256(data).hexdigest(), "matches": match})
        if not match:
            mismatches.append(path)
    write_json(destination, {"source_sha": sha, "files": records, "mismatches": mismatches})
    if not allow_overlay and mismatches:
        raise RuntimeError("source bytes differ from pinned object: " + repr(mismatches))
    return mismatches


def run_test(label, sha, source, mode):
    folder = OUT / label / mode
    folder.mkdir(parents=True, exist_ok=True)
    mismatches = source_manifest(source, sha, folder / "source-manifest-before.json", mode != "original")
    if mode != "original":
        assert set(mismatches) == {"TreeDB/internal/valuelog/manager.go", "TreeDB/internal/valuelog/stable_resource.go"}
    started = time.time()
    declared_tests = set()
    for test_source in (source / "TreeDB").glob("*_test.go"):
        declared_tests.update(re.findall(r"^func (Test[A-Za-z0-9_]+)\(", test_source.read_text(encoding="utf-8"), re.MULTILINE))
    absent_names = sorted(set(TEST_NAMES) - declared_tests)
    metadata = {"expected_top_level_order": TEST_NAMES, "source_absent_names": absent_names, "label": label, "source_sha": sha, "mode": mode, "advisory_only": mode != "original", "command": COMMAND, "cwd": str(source / "TreeDB"), "started_unix": started, "environment": {key: os.environ.get(key) for key in ["GOMEMLIMIT", "GOMAXPROCS", "GOWORK", "GOFLAGS", "CGO_ENABLED", "GOTOOLCHAIN"]}}
    write_json(folder / "invocation.json", metadata)
    print("Starting " + label + "/" + mode + " " + sha, flush=True)
    with (folder / "go-test.jsonl").open("wb") as stdout, (folder / "go-test.stderr").open("wb") as stderr:
        result = subprocess.run(COMMAND, cwd=source / "TreeDB", stdout=stdout, stderr=stderr)
    events = []
    top_level_order = []
    malformed = []
    for line in (folder / "go-test.jsonl").read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            malformed.append(line)
            continue
        if event.get("Action") == "run" and event.get("Test") and "/" not in event["Test"]:
            top_level_order.append(event["Test"])
        if event.get("Action") in ["pass", "fail", "skip"]:
            events.append(event)
    metadata.update({"exit_code": result.returncode, "elapsed_seconds": time.time() - started, "terminal_events": events, "malformed_json_lines": malformed, "observed_top_level_order": top_level_order, "top_level_order_matches_full_original_context": top_level_order == TEST_NAMES})
    write_json(folder / "result.json", metadata)
    mismatches = source_manifest(source, sha, folder / "source-manifest-after.json", mode != "original")
    if mode != "original":
        assert set(mismatches) == {"TreeDB/internal/valuelog/manager.go", "TreeDB/internal/valuelog/stable_resource.go"}
    (folder / "git-status-after.txt").write_text(capture(["git", "status", "--porcelain"], source), encoding="utf-8")
    print(label + "/" + mode + " exit=" + str(result.returncode), flush=True)
    return metadata


def instrument(source, destination):
    manager_path = source / "TreeDB/internal/valuelog/manager.go"
    stable_path = source / "TreeDB/internal/valuelog/stable_resource.go"
    manager = manager_path.read_text(encoding="utf-8")
    stable = stable_path.read_text(encoding="utf-8")
    old = '\t\topenInfo, openErr := existing.File.Stat()\n\t\tpathInfo, pathErr := os.Stat(path)'
    new = '\t\topenInfo, openErr := existing.File.Stat()\n\t\tif openErr != nil {\n\t\t\topenErr = fmt.Errorf("ADVISORY windows-handle manager.registerSegmentLocked id=%d path=%q stack=%s: %w", id, path, debug.Stack(), openErr)\n\t\t}\n\t\tpathInfo, pathErr := os.Stat(path)'
    assert manager.count(old) == 1
    manager = manager.replace(old, new).replace('\t"runtime"\n', '\t"runtime"\n\t"runtime/debug"\n', 1)
    old = 'func stableValueLogResourceToken(file *os.File, fileID uint32, registration StableResourceRegistration, namespace *rootpublication.StableNamespaceToken, contentSynced bool) (*rootpublication.StableResourceToken, error) {\n\tinfo, err := file.Stat()\n\tif err != nil {\n\t\treturn nil, err\n\t}'
    new = 'func stableValueLogResourceToken(file *os.File, fileID uint32, registration StableResourceRegistration, namespace *rootpublication.StableNamespaceToken, contentSynced bool) (*rootpublication.StableResourceToken, error) {\n\tinfo, err := file.Stat()\n\tif err != nil {\n\t\treturn nil, fmt.Errorf("ADVISORY windows-handle stableValueLogResourceToken id=%d path=%q stack=%s: %w", fileID, registration.DiagnosticPath, debug.Stack(), err)\n\t}'
    assert stable.count(old) == 1
    stable = stable.replace(old, new).replace('\t"runtime"\n', '\t"runtime"\n\t"runtime/debug"\n', 1)
    manager_path.write_text(manager, encoding="utf-8", newline="\n")
    stable_path.write_text(stable, encoding="utf-8", newline="\n")
    subprocess.run(["gofmt", "-w", str(manager_path), str(stable_path)], check=True)
    diff = subprocess.check_output(["git", "diff", "--", "TreeDB/internal/valuelog/manager.go", "TreeDB/internal/valuelog/stable_resource.go"], cwd=source)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(diff)
    return hashlib.sha256(diff).hexdigest()


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    SOURCES.mkdir(parents=True, exist_ok=False)
    assert platform.system() == "Windows", "hosted Windows only"
    assert "go1.26.8 " in capture(["go", "version"])
    write_json(OUT / "environment.json", {"platform": platform.platform(), "python": sys.version, "go_version": capture(["go", "version"]), "go_env": json.loads(capture(["go", "env", "-json"])), "diagnostic_branch_sha": capture(["git", "rev-parse", "HEAD"]), "pins": PINS, "environment": {key: value for key, value in os.environ.items() if key in ["GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "GITHUB_REF", "GITHUB_WORKFLOW_REF", "RUNNER_OS", "RUNNER_ARCH", "ImageOS", "ImageVersion", "GOMEMLIMIT", "GOMAXPROCS", "GOWORK"]}})
    (OUT / "workflow.yml").write_bytes((REPO / ".github/workflows/treedb-tests.yml").read_bytes())
    (OUT / "diagnostic-helper.py").write_bytes(Path(__file__).read_bytes())
    (OUT / "full-context.json").write_bytes(CONTEXT_PATH.read_bytes())
    for _, sha in PINS:
        subprocess.run(["git", "fetch", "--no-tags", "origin", sha], cwd=REPO, check=True)
    originals = []
    for label, sha in PINS:
        originals.append(run_test(label, sha, checkout(label + "-original", sha), "original"))
    instrumented = []
    for result in originals:
        actual_test_failure = any(e.get("Action") == "fail" and e.get("Test", "").split("/")[0] in TEST_NAMES for e in result["terminal_events"])
        if result["exit_code"] != 0 and actual_test_failure:
            label, sha = result["label"], result["source_sha"]
            source = checkout(label + "-instrumented", sha)
            overlay_hash = instrument(source, OUT / label / "instrumented-advisory" / "overlay.patch")
            observed = run_test(label, sha, source, "instrumented-advisory")
            observed["overlay_sha256"] = overlay_hash
            instrumented.append(observed)
    write_json(OUT / "receipt.json", {"originals": originals, "instrumented_advisory": instrumented, "product_gate": False, "retries": 0})
    # Any original failure remains a failed diagnostic job even if advisory rerun passes.
    return 1 if any(r["exit_code"] != 0 or not r["top_level_order_matches_full_original_context"] or r["source_absent_names"] or r["malformed_json_lines"] for r in originals) else 0


if __name__ == "__main__":
    sys.exit(main())
