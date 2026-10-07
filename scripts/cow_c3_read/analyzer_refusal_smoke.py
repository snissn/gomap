"""Synthetic incomplete packets exercise provenance refusal, never timings."""
import argparse
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

from prepare_config import draft
from protocol import C3_FIXTURE, C3_HARNESS_FILES, C3_PACKAGE, C3_PRODUCT, identity, process_environment, sha, write, validate_toolchain, selected_input_paths, digest, build_command, module_command
from build import git_source_authority

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    root = args.out.resolve()
    root.mkdir(parents=True, exist_ok=False)
    scripts = Path(__file__).resolve().parent
    original = root / "deliberately-incomplete"
    original.mkdir()
    config = draft()
    config.update(status="frozen-approved", coordinator_acceptance="synthetic refusal only, never executable collection")
    config["environment"].update(GOROOT="/synthetic/go", GOCACHE="/synthetic/cache", GOMODCACHE="/synthetic/gopath/pkg/mod", TMPDIR=str(root))
    config["host"].update(node="synthetic", machine="x86_64", release="synthetic", cpu_count=4, cpu_affinity=[0, 2, 4, 6], max_load1=1, max_load5=1, min_free_bytes=1, tmpdir=str(root), tmpdir_device=root.stat().st_dev)
    config["noise_policy"].update(max_spread_fraction=.3, material_regression_fraction=.05, minimum_effect_fraction=.1)
    toolchain = {"go_binary_sha256": "4" * 64, "executables": [
        {"path": "pkg/tool/linux_amd64/" + name, "sha256": "5" * 64, "bytes": 1, "mode": 0o755}
        for name in ("asm", "compile", "link")], "headers": [
        {"path": "pkg/include/" + name, "sha256": "6" * 64, "bytes": 1, "mode": 0o644}
        for name in ("funcdata.h", "textflag.h")],
        "go_env": {"path": "go.env", "sha256": "9" * 64, "bytes": 1, "mode": 0o644}}
    config.update(go_binary="/synthetic/go/bin/go", go_binary_sha256=toolchain["go_binary_sha256"],
                  go_version="synthetic Go version", toolchain_identity=validate_toolchain(toolchain))
    # Real tiny Git objects exercise offline provenance without pretending the
    # deliberately incomplete packet is a product measurement.
    repository, exported = root / "tiny-git-repository", root / "tiny-export"
    repository.mkdir(); exported.mkdir()
    git_env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    git_env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                   GIT_AUTHOR_NAME="Protocol smoke", GIT_AUTHOR_EMAIL="protocol-smoke@example.invalid",
                   GIT_COMMITTER_NAME="Protocol smoke", GIT_COMMITTER_EMAIL="protocol-smoke@example.invalid")
    def git(*argv):
        return subprocess.check_output(["git", "-C", str(repository), *argv], env=git_env, stderr=subprocess.PIPE).decode().strip()
    git("init", "-q")
    (repository / "fixture.go").write_text("synthetic protocol input\n")
    (exported / "fixture.go").write_text("synthetic protocol input\n")
    for tree in (repository, exported):
        for name in C3_HARNESS_FILES:
            fixture = tree / name
            fixture.parent.mkdir(parents=True, exist_ok=True)
            fixture.write_text("synthetic canonical workload input " + name + "\n")
        target = tree / "scripts" / "cow_c3_read"
        target.mkdir(parents=True)
        for name in ("protocol.py", "collect.py", "analyze.py", "build.py"):
            shutil.copyfile(scripts / name, target / name)
    git("add", "."); git("commit", "-qm", "Protocol refusal input")
    for fixture in config["fixtures"]:
        fixture["sha256"] = sha(exported / fixture["path"])
    for index, variant in enumerate(("baseline", "candidate"), 1):
        if variant == "candidate":
            for source in (repository, exported):
                (source / "fixture.go").write_text("distinct synthetic candidate input\n")
            git("add", "."); git("commit", "-qm", "Distinct refusal candidate")
        head, tree = git("rev-parse", "HEAD"), git("rev-parse", "HEAD^{tree}")
        git_manifest, git_receipt = git_source_authority(exported, repository, head, tree)
        declaration = config["variants"][variant]
        declaration.update(production_commit=head, production_git_tree=tree, source="/synthetic/" + variant,
                           binary="/synthetic/" + variant + "-build/mvcc.test", binary_sha256=str(index) * 64)
        manifest = original / (variant + "-source-manifest.json")
        write(manifest, git_manifest)
        bound = identity(manifest)
        declaration.update(manifest="/synthetic/" + variant + "-build/source-manifest.json", manifest_sha256=sha(manifest), source_tree_sha256=bound["tree_sha256"])
        write(original / (variant + "-identity.json"), bound)
        write(original / (variant + "-source-before.json"), {"drift": []})
        write(original / (variant + "-build-source-after.json"), {"drift": []})
        required = {"go_env", "module_graph", "effective_module_graph", "compiled_dependencies", "binary_buildinfo", "build_stdout", "build_stderr", "compiled_input_closure", "compiled_inputs_before", "generated_nonpersistent_inputs", "git_source", "toolchain"}
        packages = [{"ImportPath": C3_PRODUCT, "Dir": declaration["source"], "GoFiles": ["fixture.go"], "Deps": ["synthetic/standard"]},
                    {"ImportPath": "synthetic/standard", "Standard": True, "Dir": "/synthetic/go/src/standard", "GoFiles": ["standard.go"]},
                    {"ImportPath": C3_PACKAGE, "Dir": declaration["source"] + "/TreeDB/mvcc/cowbench",
                     "GoFiles": [Path(name).name for name in C3_HARNESS_FILES if name.startswith("TreeDB/mvcc/cowbench/")]},
                    {"ImportPath": "github.com/snissn/gomap/TreeDB/internal/cowbench", "Dir": declaration["source"] + "/TreeDB/internal/cowbench", "GoFiles": ["admission.go"]}]
        selected, generated = selected_input_paths(packages, declaration["source"], config["environment"])
        closure = {key: {"path": value, "sha256": "7" * 64, "bytes": 1, "mode": 0o644} for key, value in selected.items()}
        for name in C3_HARNESS_FILES:
            closure["REPO/" + name].update(sha256=sha(exported / name), bytes=(exported / name).stat().st_size)
        external = {key: {k: value[k] for k in ("sha256", "bytes", "mode")} for key, value in closure.items() if not key.startswith("REPO/")}
        config["external_input_identity"] = digest(external)
        retained, artifacts = {}, {}
        for name in sorted(required):
            path = original / (variant + "-" + name + ".raw")
            if name == "git_source":
                write(path, git_receipt)
            elif name == "toolchain":
                write(path, toolchain)
            elif name == "compiled_dependencies":
                path.write_text("\n".join(json.dumps(item) for item in packages) + "\n")
            elif name in ("compiled_input_closure", "compiled_inputs_before"):
                write(path, closure)
            elif name == "generated_nonpersistent_inputs":
                write(path, generated)
            elif name == "go_env":
                effective = process_environment(config["environment"])
                write(path, {**{key: effective[key] for key in ("GOROOT", "GOFLAGS", "GOWORK", "GOCACHE", "GOMODCACHE", "GOENV", "GOTOOLCHAIN", "GOPATH", "CGO_ENABLED", "GOAMD64", "GOEXPERIMENT")}, "GOENV": "", "GOOS": "linux", "GOARCH": "amd64"})
            else:
                path.write_text("synthetic-refusal-input " + name + "\n")
            retained[name] = {"path": path.name, "sha256": sha(path)}
            artifacts[name] = {"path": "/synthetic/build/" + name, "sha256": sha(path)}
        write(original / (variant + "-build-artifacts.json"), retained)
        receipt = original / (variant + "-build-receipt.json")
        write(receipt, {"binary_sha256": declaration["binary_sha256"], "source_tree_sha256": declaration["source_tree_sha256"],
                        "environment": config["environment"], "effective_process_environment": process_environment(config["environment"]),
                        "go_version": config["go_version"], "go_binary_sha256": config["go_binary_sha256"], "toolchain_identity": config["toolchain_identity"],
                        "suite": "c3", "race": False, "build_tags": [], "artifacts": artifacts,
                        "effective_module_identity": retained["effective_module_graph"]["sha256"],
                        "exit_code": 0, "command": build_command(config["go_binary"], declaration["binary"]),
                        "module_producer_command": module_command(config["go_binary"]),
                        "harness_input_identity": digest(config["fixtures"]), "external_input_identity": config["external_input_identity"],
                        **{field: artifacts[name]["sha256"] for field, name in (("compiled_inputs_before_sha256", "compiled_inputs_before"),
                        ("compiled_input_closure_sha256", "compiled_input_closure"), ("generated_nonpersistent_inputs_sha256", "generated_nonpersistent_inputs"))}})
        declaration.update(build_receipt="/synthetic/" + variant + "-build/build-receipt.json", build_receipt_sha256=sha(receipt))
    write(original / "config.json", config)
    write(original / "live-toolchain.json", {"go_version": config["go_version"], "inventory": toolchain})
    write(original / "environment.json", {"effective_controls": config["environment"], "effective_process_environment": process_environment(config["environment"])})
    write(original / "receipts.json", [])
    hashes = {}
    for name in ("protocol.py", "collect.py", "analyze.py", "build.py"):
        shutil.copyfile(scripts / name, original / name)
        hashes[name] = sha(original / name)
    write(original / "script-identity.json", hashes)
    write(original / "completion.json", {"config_sha256": sha(original / "config.json"), "receipts_sha256": sha(original / "receipts.json"),
                                         "script_identity_sha256": sha(original / "script-identity.json"), "runs": 756})
    def damage_build_environment(packet):
        receipt = packet / "baseline-build-receipt.json"
        value = json.loads(receipt.read_text())
        value["effective_process_environment"]["GOAMD64"] = "v3"
        write(receipt, value)
        rebind_receipt(packet, receipt)

    def rebind_receipt(packet, receipt):
        frozen = json.loads((packet / "config.json").read_text())
        frozen["variants"]["baseline"]["build_receipt_sha256"] = sha(receipt)
        write(packet / "config.json", frozen)
        completion = json.loads((packet / "completion.json").read_text())
        completion["config_sha256"] = sha(packet / "config.json")
        write(packet / "completion.json", completion)

    def damage_go_environment(packet, key="GOENV", setting="/synthetic/persisted-goenv"):
        raw = packet / "baseline-go_env.raw"
        value = json.loads(raw.read_text())
        value[key] = setting
        write(raw, value)
        artifacts = json.loads((packet / "baseline-build-artifacts.json").read_text())
        artifacts["go_env"]["sha256"] = sha(raw)
        write(packet / "baseline-build-artifacts.json", artifacts)
        receipt = packet / "baseline-build-receipt.json"
        value = json.loads(receipt.read_text())
        value["artifacts"]["go_env"]["sha256"] = sha(raw)
        write(receipt, value)
        rebind_receipt(packet, receipt)

    def damage_fixture(packet):
        frozen = json.loads((packet / "config.json").read_text())
        frozen["fixtures"][0]["sha256"] = "0" * 64
        write(packet / "config.json", frozen)
        completion = json.loads((packet / "completion.json").read_text())
        completion["config_sha256"] = sha(packet / "config.json")
        write(packet / "completion.json", completion)

    def damage_toolchain(packet, header=False, go_env=False):
        raw = packet / "baseline-toolchain.raw"
        value = json.loads(raw.read_text())
        if go_env:
            value["go_env"]["sha256"] = "8" * 64
        else:
            value["headers" if header else "executables"][0 if header else 1]["sha256"] = "8" * 64
        write(raw, value)
        artifacts = json.loads((packet / "baseline-build-artifacts.json").read_text())
        artifacts["toolchain"]["sha256"] = sha(raw)
        write(packet / "baseline-build-artifacts.json", artifacts)
        receipt = packet / "baseline-build-receipt.json"
        build = json.loads(receipt.read_text())
        build["artifacts"]["toolchain"]["sha256"] = sha(raw)
        build["toolchain_identity"] = validate_toolchain(value)
        write(receipt, build)
        rebind_receipt(packet, receipt)

    def damage_build_toolchain(packet, key, value):
        receipt = packet / "baseline-build-receipt.json"
        build = json.loads(receipt.read_text()); build[key] = value
        write(receipt, build); rebind_receipt(packet, receipt)

    def damage_compiled_cgo(packet):
        raw = packet / "baseline-compiled_dependencies.raw"
        write(raw, {"ImportPath": "synthetic/cgo", "CgoFiles": ["fixture.go"]})
        artifacts = json.loads((packet / "baseline-build-artifacts.json").read_text())
        artifacts["compiled_dependencies"]["sha256"] = sha(raw)
        write(packet / "baseline-build-artifacts.json", artifacts)
        receipt = packet / "baseline-build-receipt.json"
        build = json.loads(receipt.read_text())
        build["artifacts"]["compiled_dependencies"]["sha256"] = sha(raw)
        write(receipt, build); rebind_receipt(packet, receipt)

    def damage_same_product(packet):
        frozen = json.loads((packet / "config.json").read_text())
        frozen["variants"]["candidate"] = copy.deepcopy(frozen["variants"]["baseline"])
        write(packet / "config.json", frozen)
        completion = json.loads((packet / "completion.json").read_text())
        completion["config_sha256"] = sha(packet / "config.json")
        write(packet / "completion.json", completion)

    def damage_live_toolchain(packet, field):
        path = packet / "live-toolchain.json"
        value = json.loads(path.read_text())
        if field == "version":
            value["go_version"] = "different live version"
        elif field == "launcher":
            value["inventory"]["go_binary_sha256"] = "7" * 64
        elif field == "header":
            value["inventory"]["headers"][0]["sha256"] = "8" * 64
        elif field == "go-env":
            value["inventory"]["go_env"]["sha256"] = "8" * 64
        else:
            value["inventory"]["executables"][1]["sha256"] = "6" * 64
        write(path, value)

    def damage_inputs(packet, kind):
        receipt = packet / "baseline-build-receipt.json"
        build = json.loads(receipt.read_text())
        artifacts = json.loads((packet / "baseline-build-artifacts.json").read_text())
        names = ("compiled_input_closure",) if kind == "build-drift" else ("compiled_inputs_before", "compiled_input_closure")
        for name in names:
            raw = packet / ("baseline-" + name + ".raw")
            value = json.loads(raw.read_text())
            key = next(key for key in value if key.startswith("GOROOT/"))
            if kind == "missing":
                del value[key]
            elif kind == "mode":
                value[key]["mode"] = 0o600
            else:
                value[key]["sha256"] = "8" * 64
            write(raw, value)
            artifacts[name]["sha256"] = build["artifacts"][name]["sha256"] = sha(raw)
            build["compiled_inputs_before_sha256" if name == "compiled_inputs_before" else "compiled_input_closure_sha256"] = sha(raw)
        if kind in ("mode", "bytes"):
            external = {key: {k: value[k] for k in ("sha256", "bytes", "mode")} for key, value in value.items() if not key.startswith("REPO/")}
            build["external_input_identity"] = digest(external)
        write(packet / "baseline-build-artifacts.json", artifacts)
        write(receipt, build)
        rebind_receipt(packet, receipt)

    def damage_module_identity(packet, variant, missing=False, rehash_graph=False):
        receipt = packet / (variant + "-build-receipt.json")
        build = json.loads(receipt.read_text())
        if rehash_graph:
            raw = packet / (variant + "-effective_module_graph.raw")
            raw.write_text("distinct self-consistent effective module graph\n")
            artifacts_path = packet / (variant + "-build-artifacts.json")
            artifacts = json.loads(artifacts_path.read_text())
            artifacts["effective_module_graph"]["sha256"] = sha(raw)
            build["artifacts"]["effective_module_graph"]["sha256"] = sha(raw)
            build["effective_module_identity"] = sha(raw)
            write(artifacts_path, artifacts)
        elif missing:
            del build["effective_module_identity"]
        else:
            build["effective_module_identity"] = "0" * 64
        write(receipt, build)
        frozen = json.loads((packet / "config.json").read_text())
        frozen["variants"][variant]["build_receipt_sha256"] = sha(receipt)
        write(packet / "config.json", frozen)
        completion = json.loads((packet / "completion.json").read_text())
        completion["config_sha256"] = sha(packet / "config.json")
        write(packet / "completion.json", completion)

    cases = [
        ("incomplete-runs", None, "missing/extra runs"),
        ("empty-map", lambda packet: write(packet / "baseline-build-artifacts.json", {}), "missing/extra build provenance map"),
        ("missing-map", lambda packet: (packet / "baseline-build-artifacts.json").unlink(), "FileNotFoundError"),
        ("changed-receipt", lambda packet: (packet / "baseline-build-receipt.json").write_text("{}\n"), "build receipt drift"),
        ("changed-identity", lambda packet: write(packet / "baseline-identity.json", {}), "source identity drift"),
        ("changed-manifest", lambda packet: write(packet / "baseline-source-manifest.json", {"files": [{"path": "fixture.go", "sha256": "4" * 64}]}), "source identity drift"),
        ("changed-artifact", lambda packet: (packet / "baseline-compiled_input_closure.raw").write_text("changed\n"), "build provenance drift"),
        ("build-source-drift", lambda packet: write(packet / "baseline-build-source-after.json", {"drift": ["fixture.go"]}), "build source-after drift"),
        ("captured-process-environment", lambda packet: write(packet / "environment.json", {}), "captured process environment mismatch"),
        ("build-process-environment", damage_build_environment, "build process environment mismatch"),
        ("actual-go-environment", damage_go_environment, "actual go env mismatch GOENV"),
        ("actual-go-cgo-enabled", lambda p: damage_go_environment(p, "CGO_ENABLED", "1"), "actual go env mismatch CGO_ENABLED"),
        ("actual-go-amd64", lambda p: damage_go_environment(p, "GOAMD64", "v3"), "actual go env mismatch GOAMD64"),
        ("actual-go-experiments", lambda p: damage_go_environment(p, "GOEXPERIMENT", "arenas"), "actual go env mismatch GOEXPERIMENT"),
        ("wrong-frozen-fixture", damage_fixture, "fixture differs from frozen source"),
        ("same-product", damage_same_product, "distinct production_commit"),
        ("rehashed-toolchain", damage_toolchain, "Go toolchain inventory mismatch"),
        ("rehashed-assembler-header", lambda p: damage_toolchain(p, True), "Go toolchain inventory mismatch"),
        ("rehashed-go-env", lambda p: damage_toolchain(p, go_env=True), "Go toolchain inventory mismatch"),
        ("rehashed-go-version", lambda p: damage_build_toolchain(p, "go_version", "different"), "toolchain mismatch"),
        ("rehashed-go-launcher", lambda p: damage_build_toolchain(p, "go_binary_sha256", "7" * 64), "toolchain mismatch"),
        ("rehashed-compiled-cgo", damage_compiled_cgo, "compiled Cgo inputs forbidden"),
        ("rehashed-external-bytes", lambda p: damage_inputs(p, "bytes"), "actual external compiler inputs differ"),
        ("rehashed-external-mode", lambda p: damage_inputs(p, "mode"), "actual external compiler inputs differ"),
        ("rehashed-build-input-drift", lambda p: damage_inputs(p, "build-drift"), "selected persistent inputs drift"),
        ("rehashed-missing-persistent-input", lambda p: damage_inputs(p, "missing"), "selected persistent input closure mismatch"),
        ("missing-live-toolchain", lambda p: (p / "live-toolchain.json").unlink(), "FileNotFoundError"),
        ("changed-live-version", lambda p: damage_live_toolchain(p, "version"), "live toolchain version mismatch"),
        ("changed-live-launcher", lambda p: damage_live_toolchain(p, "launcher"), "Go toolchain inventory mismatch"),
        ("changed-live-assembler-header", lambda p: damage_live_toolchain(p, "header"), "Go toolchain inventory mismatch"),
        ("changed-live-go-env", lambda p: damage_live_toolchain(p, "go-env"), "Go toolchain inventory mismatch"),
        ("changed-live-compiler", lambda p: damage_live_toolchain(p, "compiler"), "Go toolchain inventory mismatch"),
    ]
    for variant in ("baseline", "candidate"):
        for label, missing in (("missing", True), ("unbound", False)):
            cases.append((variant + "-module-identity-" + label,
                          lambda p, v=variant, m=missing: damage_module_identity(p, v, m),
                          "unbound canonical effective module graph"))
    cases.append(("rehashed-distinct-module-graph",
                  lambda p: damage_module_identity(p, "candidate", rehash_graph=True), "effective module graph differs"))
    argv = build_command(config["go_binary"], config["variants"]["baseline"]["binary"])
    for label, command_value in (
            ("gcflags", argv[:2] + ["-gcflags=all=-N -l"] + argv[2:]),
            ("ldflags", argv[:2] + ["-ldflags=-s -w"] + argv[2:]),
            ("race", argv[:2] + ["-race"] + argv[2:]),
            ("missing-trimpath", [arg for arg in argv if arg != "-trimpath"]),
            ("output", argv[:5] + ["/synthetic/other-binary"] + argv[6:]),
            ("package", argv[:-1] + ["./TreeDB"]),
            ("launcher", ["/synthetic/other-go"] + argv[1:]),
            ("order", argv[:2] + [argv[3], argv[2]] + argv[4:]),
            ("empty", [])):
        cases.append(("rehashed-build-" + label,
                      lambda p, a=command_value: damage_build_toolchain(p, "command", a), "actual ordinary build invocation mismatch"))
    for label, exit_value in (("failed", 1), ("bool", False), ("float", 0.0)):
        cases.append(("rehashed-build-exit-" + label,
                      lambda p, v=exit_value: damage_build_toolchain(p, "exit_code", v), "actual ordinary build invocation mismatch"))
    cases.append(("rehashed-module-producer", lambda p: damage_build_toolchain(p, "module_producer_command",
                  [config["go_binary"], "list", "-deps", "-test", "-json", "./TreeDB/mvcc"]),
                  "actual module producer invocation mismatch"))
    results = []
    for label, mutation, expected in cases:
        packet = root / label
        shutil.copytree(original, packet)
        if mutation:
            mutation(packet)
        process = subprocess.run([sys.executable, "-B", str(scripts / "analyze.py"), str(packet)], capture_output=True)
        (root / (label + ".stdout")).write_bytes(process.stdout)
        (root / (label + ".stderr")).write_bytes(process.stderr)
        assert process.returncode != 0 and expected in process.stderr.decode(), (label, process.stderr.decode())
        results.append({"label": label, "exit_code": process.returncode, "expected_refusal": expected})
    write(root / "result.json", {"scope": "synthetic incomplete/damaged packet refusal only; no performance samples or acceptance", "results": results,
                                "script_sha256": sha(Path(__file__)), "analyzer_sha256": sha(scripts / "analyze.py")})
    print(json.dumps({"refusals": len(results)}))

if __name__ == "__main__":
    main()
