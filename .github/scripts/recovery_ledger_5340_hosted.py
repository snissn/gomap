import datetime
import hashlib
import json
import os
import pathlib
import platform
import re
import subprocess
import sys

# Source-only authored runner; no local import/compile/execution during construction.

candidate = "bd614f5701b16bcb16f11c4e52806626aa6bbd4c"
expected_tree = "9614c41a01e89491caf4b2f1ae6274f92e18104d"
basis = "6fe6f26ac3ffbc5a02c78d3417dd85794b458aee"
root = pathlib.Path(os.environ["GITHUB_WORKSPACE"])
product, harness = root / "product", root / "harness"
cell = os.environ["LEDGER_CELL"]
mode = os.environ["LEDGER_MODE"]
assert mode in ("controls", "default", "diagnostic")
packet = pathlib.Path(os.environ["RUNNER_TEMP"]) / ("recovery-ledger-5340-" + cell + "-" + mode)
packet.mkdir(exist_ok=False)
(packet / "bin").mkdir()
temporary = pathlib.Path(os.environ["RUNNER_TEMP"]) / ("recovery-ledger-tmp-" + cell)
temporary.mkdir(exist_ok=False)
env = os.environ.copy()
env.pop("GOMAP_RECOVERY_LEDGER", None)
env.pop("GOMAP_ORDERED_ACK_IMAGE_CHILD", None)
env["TMPDIR"] = env["TMP"] = env["TEMP"] = str(temporary)
receipt = {"mode": "DIAGNOSTIC_ONLY_NOT_PERFORMANCE_OR_REPAIR", "cell": cell, "invocation": mode,
           "candidate": candidate, "tree": expected_tree, "basis": basis,
           "harness_event_sha": env["GITHUB_SHA"], "commands": [], "tests": []}
def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
def save():
    pending = packet / "receipt.pending"
    pending.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    pending.replace(packet / "receipt.json")
def digest(path):
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    return {"bytes": path.stat().st_size, "sha256": hasher.hexdigest()}
def git(directory, *arguments):
    return subprocess.check_output(["git", "-C", str(directory), *arguments], env=env).decode("utf-8").strip()
def call(label, argv, execution_env=None):
    log = packet / (label + ".log")
    entry = {"label": label, "argv": argv, "cwd": str(product), "start": now(), "status": "RUNNING"}
    receipt["commands"].append(entry)
    save()
    with log.open("wb") as output:
        process = subprocess.Popen(argv, cwd=product, env=execution_env or env,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in iter(process.stdout.readline, b""):
            output.write(line)
            output.flush()
            sys.stdout.buffer.write(line)
            sys.stdout.buffer.flush()
        process.stdout.close()
        code = process.wait()
    entry.update({"end": now(), "exit_code": code, "status": "PASS" if code == 0 else "FAIL", "log": digest(log)})
    save()
    return code
save()
assert git(product, "rev-parse", "HEAD") == candidate
assert git(product, "rev-parse", "HEAD^{tree}") == expected_tree
assert git(harness, "rev-parse", "HEAD") == env["GITHUB_SHA"]
assert git(harness, "diff", "--name-only", candidate, "HEAD").splitlines() == [".github/ci/ci_impact.json", ".github/scripts/recovery_ledger_5340_hosted.py", ".github/workflows/treedb-tests.yml"]
assert env["GITHUB_REF"] == "refs/heads/prepared/5340-hosted-ledger-20261030"
assert env["GITHUB_RUN_ATTEMPT"] == "1"
assert env.get("RUNNER_ENVIRONMENT") == "github-hosted"
assert cell in ("linux-normal", "windows-normal", "linux-race")
assert os.environ["LEDGER_RACE"] == ("1" if cell == "linux-race" else "0")
assert (platform.system() == "Windows") == (cell == "windows-normal")
assert git(product, "status", "--porcelain", "--untracked-files=all") == ""
assert git(harness, "status", "--porcelain", "--untracked-files=all") == ""
receipt["product_parents"] = git(product, "show", "-s", "--format=%P", "HEAD")
receipt["event"] = {key: env.get(key) for key in ("GITHUB_EVENT_NAME", "GITHUB_REF", "GITHUB_WORKFLOW_REF", "GITHUB_WORKFLOW_SHA", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_JOB", "RUNNER_ENVIRONMENT", "RUNNER_OS", "RUNNER_ARCH", "ImageOS", "ImageVersion")}
receipt["environment"] = {key: env.get(key) for key in ("GOWORK", "GOMAXPROCS", "GOMEMLIMIT", "GODEBUG", "CGO_ENABLED", "GOOS", "GOARCH", "GOTOOLCHAIN", "GOMAP_RECOVERY_LEDGER", "GOMAP_ORDERED_ACK_IMAGE_CHILD")}
receipt["platform"] = platform.platform()
workflow = harness / ".github/workflows/treedb-tests.yml"
(packet / "workflow.yml").write_bytes(workflow.read_bytes())
(packet / "runner.py").write_bytes(Path(__file__).read_bytes())
(packet / "candidate.patch").write_text(git(product, "diff", "--binary", basis, candidate) + "\n", encoding="utf-8")
(packet / "candidate-tree.txt").write_text(git(product, "ls-tree", "-r", "--full-tree", "HEAD") + "\n", encoding="utf-8")
changed = git(product, "diff", "--name-only", basis, candidate).splitlines()
expected_changed = [".github/ci/ci_impact.json", "TreeDB/db/durable_root_runtime.go", "TreeDB/db/recovery_trace_test.go", "TreeDB/db/root_publication_activation.go", "TreeDB/db/wal_recovery.go", "TreeDB/docs/diagnostics/windows-ack-recovery-ledger.md", "TreeDB/internal/recoverytrace/ledger.go", "TreeDB/internal/recoverytrace/ledger_test.go", "TreeDB/internal/rootpublication/resource_sync_observer_test.go", "TreeDB/internal/rootpublication/resource_token.go", "TreeDB/ordered_completion_stable_image_test.go", "TreeDB/ordered_recovery_ledger_test.go"]
assert changed == expected_changed
receipt["source_files"] = {}
for name in changed:
    original = product / name
    copied = packet / "sources" / name
    copied.parent.mkdir(parents=True, exist_ok=True)
    copied.write_bytes(original.read_bytes())
    receipt["source_files"][name] = {"blob": git(product, "rev-parse", candidate + ":" + name), **digest(copied)}
entries = subprocess.check_output(["git", "-C", str(product), "ls-tree", "-rz", "--full-tree", "HEAD"], env=env).split(b"\0")
discovery = hashlib.sha256()
classified = 0
for entry in entries:
    if not entry:
        continue
    descriptor, encoded_path = entry.split(b"\t", 1)
    mode, kind, blob = descriptor.decode().split()
    path = encoded_path.decode()
    if path.endswith((".go", ".py", ".sh", ".s", ".S", ".c", ".h", ".cc", ".cpp", ".cxx", ".hpp", ".hh", ".m", ".mm", ".syso", ".mk", "go.mod", "go.sum")) or path.rsplit("/", 1)[-1].startswith("go.work") or path == "Makefile" or mode == "100755":
        discovery.update(encoded_path + b"\0" + mode.encode() + b"\0" + blob.encode() + b"\0")
        classified += 1
assert classified == 5028 and len(entries) - 1 == 12698
assert discovery.hexdigest() == "5fc9dd30215f5bbce32fb0523cdbd57fd43ac75bce35c3d1c9f768cf8f93fa20"
policy = json.loads((product / ".github/ci/ci_impact.json").read_text())
assert policy["discovery_source_sha256"] == discovery.hexdigest()
receipt["source_inventory"] = {"classified": classified, "total": len(entries) - 1, "sha256": discovery.hexdigest(), "note": "H workflow intentionally differs; H is not a normal-policy qualification head"}
save()
if call("go-version", ["go", "version"]) != 0 or call("go-env", ["go", "env", "-json", "GOOS", "GOARCH", "CGO_ENABLED", "GOWORK", "GOTOOLCHAIN"]) != 0:
    raise SystemExit("compiler metadata failed; no tests admitted")
assert (packet / "go-version.log").read_text().startswith("go version go1.26.10 ")
selections = [
    ("recoverytrace", "./TreeDB/internal/recoverytrace", "^(TestLedgerBoundedOwnershipAndOverflow|TestLedgerSingleInstallationAndRestore|TestLedgerConcurrentPublicationAndRead)$"),
    ("rootpublication", "./TreeDB/internal/rootpublication", "^(TestStableResourceFileSyncObserverExactPhysicalBoundary|TestRecoveryLedgerPhysicalAbnormalExitHasNoReturn)$"),
    ("db", "./TreeDB/db", "^TestRecoveryLedgerStoragePhaseErrorsPreserveOriginalOrder$"),
    ("treedb", "./TreeDB", "^TestACKRecoveryLedgerRejectsOriginalPublicOpenFailure$"),
]
suffix = ".test.exe" if platform.system() == "Windows" else ".test"
tests = [{"label": name + "-controls", "binary": name, "selector": selector, "ledger": False, "status": "NOT_RUN"} for name, package, selector in selections]
if mode != "controls":
    tests = [{"label": "ack-" + mode, "binary": "treedb", "selector": "^TestOrderedCompletionStableImageAtAcknowledgement$", "ledger": mode == "diagnostic", "status": "NOT_RUN"}]
required = {
    "recoverytrace-controls": ["TestLedgerBoundedOwnershipAndOverflow", "TestLedgerSingleInstallationAndRestore", "TestLedgerConcurrentPublicationAndRead"],
    "rootpublication-controls": ["TestStableResourceFileSyncObserverExactPhysicalBoundary", "TestRecoveryLedgerPhysicalAbnormalExitHasNoReturn"] + ["TestStableResourceFileSyncObserverExactPhysicalBoundary/" + name for name in ("direct", "imported", "covered", "before-error", "sync-error", "after-error", "imported-after-error")] + ["TestRecoveryLedgerPhysicalAbnormalExitHasNoReturn/" + name for name in ("panic", "goexit")],
    "db-controls": ["TestRecoveryLedgerStoragePhaseErrorsPreserveOriginalOrder"] + ["TestRecoveryLedgerStoragePhaseErrorsPreserveOriginalOrder/" + name for name in ("materialize", "index", "install")],
    "treedb-controls": ["TestACKRecoveryLedgerRejectsOriginalPublicOpenFailure"],
}
ack_names = ["TestOrderedCompletionStableImageAtAcknowledgement"] + ["TestOrderedCompletionStableImageAtAcknowledgement/" + name for name in ("fifo-1", "fifo-70", "later-driver")]
required.update({"ack-default": ack_names, "ack-diagnostic": ack_names})
for test in tests:
    test["required_pass"] = required[test["label"]]
def test_result(test, code):
    lines = (packet / (test["label"] + ".log")).read_text(encoding="utf-8", errors="replace").splitlines()
    missing = [name for name in test["required_pass"] if not any(line.lstrip().startswith("--- PASS: " + name + " (") for line in lines)]
    test["missing_pass"] = missing
    test["exit_code"] = code
    test["status"] = "PASS" if code == 0 and not missing else "FAIL_OR_MISSING_WITNESS"
    if test["ledger"]:
        text = "\n".join(lines)
        activated = text.count("recovery-ledger-summary final=true capacity=4096") == 3 and text.count("stage=2 outcome=1") >= 3
        test["diagnostic_activated"] = activated
        test["observation"] = "REQUIRES_REVIEW_NOT_GUARANTEED_COMPLETE"
        test["reported_nonzero_drops"] = any(int(value) != 0 for value in re.findall(r"dropped=(\d+)", text))
        test["reported_clipped_label"] = "label-truncated=true" in text
        if not activated:
            test["status"] = "FAIL_OR_MISSING_WITNESS"
    save()
    return test["status"] == "PASS"
receipt["tests"] = tests
receipt["binaries"] = {}
failed = False
try:
    if mode == "controls":
        for name, package, selector in selections:
            binary = packet / "bin" / (name + suffix)
            command = ["go", "test", "-c", "-o", str(binary)]
            if cell == "linux-race":
                command.append("-race")
            command.append(package)
            if call("compile-" + name, command) != 0:
                failed = True
                break
            with binary.open("rb") as stream:
                assert stream.read(4).startswith(b"MZ" if suffix.endswith("exe") else b"\x7fELF")
            receipt["binaries"][name] = {"path": str(binary.relative_to(packet)), "package": package, "race": cell == "linux-race", "format_expected": "PE" if suffix.endswith("exe") else "ELF", **digest(binary)}
            save()
            if call("module-info-" + name, ["go", "version", "-m", str(binary)]) != 0 or call("build-id-" + name, ["go", "tool", "buildid", str(binary)]) != 0:
                failed = True
                break
        if not failed:
            for test in tests:
                binary = packet / receipt["binaries"][test["binary"]]["path"]
                code = call(test["label"], [str(binary), "-test.run=" + test["selector"], "-test.count=1", "-test.timeout=2m", "-test.v"])
                if not test_result(test, code):
                    failed = True
                    break
    else:
        # Each observation gets a fresh hosted job/VM. Default RED cannot leave
        # process custody shared with or suppress the instrumented observation.
        inputs = pathlib.Path(env["LEDGER_INPUTS"])
        controls = json.loads((inputs / "receipt.json").read_text())
        assert controls["candidate"] == candidate and controls["tree"] == expected_tree
        assert controls["harness_event_sha"] == env["GITHUB_SHA"] and controls["cell"] == cell
        assert controls["invocation"] == "controls" and controls["verdict"] == "FOCUSED_EXECUTION_PASS_NOT_CAUSAL_CLOSURE"
        assert len(controls["tests"]) == 4 and all(test["status"] == "PASS" for test in controls["tests"])
        assert controls["clean_product_after"] is True
        for key in ("RUNNER_OS", "RUNNER_ARCH", "ImageOS", "ImageVersion"):
            assert controls["event"].get(key) == env.get(key), "builder/runtime image drift: " + key
        descriptor = controls["binaries"]["treedb"]
        assert descriptor["path"] == "bin/treedb" + suffix
        binary = inputs / descriptor["path"]
        assert digest(binary) == {key: descriptor[key] for key in ("bytes", "sha256")}
        if platform.system() != "Windows":
            binary.chmod(0o700)  # artifact extraction normalizes executable mode
        receipt["input_controls_receipt"] = digest(inputs / "receipt.json")
        receipt["input_controls_artifact"] = env["LEDGER_INPUT_ARTIFACT"]
        receipt["binaries"]["treedb"] = descriptor
        execution_env = env.copy()
        if mode == "diagnostic":
            execution_env["GOMAP_RECOVERY_LEDGER"] = "1"
        test = tests[0]
        code = call(test["label"], [str(binary), "-test.run=" + test["selector"], "-test.count=1", "-test.timeout=2m", "-test.v"], execution_env)
        failed = not test_result(test, code)
    receipt["clean_product_after"] = git(product, "status", "--porcelain", "--untracked-files=all") == ""
    failed = failed or not receipt["clean_product_after"]
except BaseException as error:
    failed = True
    receipt["exception_type"] = type(error).__name__
    raise
finally:
    receipt["end"] = now()
    receipt["verdict"] = "FAIL_OR_INCOMPLETE" if failed or any(test["status"] != "PASS" for test in tests) else "FOCUSED_EXECUTION_PASS_NOT_CAUSAL_CLOSURE"
    save()
    inventory = {str(path.relative_to(packet)): digest(path) for path in sorted(packet.rglob("*")) if path.is_file() and path.name != "artifact-inventory.json"}
    (packet / "artifact-inventory.json").write_text(json.dumps(inventory, indent=2) + "\n", encoding="utf-8")
raise SystemExit(1 if failed else 0)
