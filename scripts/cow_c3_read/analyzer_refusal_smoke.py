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
from protocol import C3_FIXTURE, identity, process_environment, sha, write, validate_toolchain
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
    config["host"].update(node="synthetic", machine="x86_64", release="synthetic", cpu_count=4, max_load1=1, max_load5=1, min_free_bytes=1, tmpdir=str(root), tmpdir_device=root.stat().st_dev)
    config["noise_policy"].update(max_spread_fraction=.3, material_regression_fraction=.05, minimum_effect_fraction=.1)
    toolchain = {"go_binary_sha256": "4" * 64, "executables": [
        {"path": "pkg/tool/linux_amd64/" + name, "sha256": "5" * 64, "bytes": 1, "mode": 0o755}
        for name in ("asm", "compile", "link")]}
    config.update(go_binary="/synthetic/go/bin/go", go_binary_sha256=toolchain["go_binary_sha256"],
                  go_version="synthetic Go version", toolchain_identity=validate_toolchain(toolchain))
    # Real tiny Git objects exercise offline provenance without pretending the
    # deliberately incomplete packet is a product measurement.
    repository, exported = root / "tiny-git-repository", root / "tiny-export"
    repository.mkdir(); exported.mkdir()
    git_env = dict(os.environ, GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                   GIT_AUTHOR_NAME="Protocol smoke", GIT_AUTHOR_EMAIL="protocol-smoke@example.invalid",
                   GIT_COMMITTER_NAME="Protocol smoke", GIT_COMMITTER_EMAIL="protocol-smoke@example.invalid")
    def git(*argv):
        return subprocess.check_output(["git", "-C", str(repository), *argv], env=git_env, stderr=subprocess.PIPE).decode().strip()
    git("init", "-q")
    (repository / "fixture.go").write_text("synthetic protocol input\n")
    (exported / "fixture.go").write_text("synthetic protocol input\n")
    for tree in (repository, exported):
        fixture = tree / C3_FIXTURE
        fixture.parent.mkdir(parents=True)
        fixture.write_text("synthetic canonical workload input\n")
        target = tree / "scripts" / "cow_c3_read"
        target.mkdir(parents=True)
        for name in ("protocol.py", "collect.py", "analyze.py", "build.py"):
            shutil.copyfile(scripts / name, target / name)
    git("add", "."); git("commit", "-qm", "Protocol refusal input")
    config["fixtures"][0]["sha256"] = sha(exported / C3_FIXTURE)
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
        required = {"go_env", "module_graph", "effective_module_graph", "compiled_dependencies", "binary_buildinfo", "build_stdout", "build_stderr", "compiled_input_closure", "generated_nonpersistent_inputs", "git_source", "toolchain"}
        retained, artifacts = {}, {}
        for name in sorted(required):
            path = original / (variant + "-" + name + ".raw")
            if name == "git_source":
                write(path, git_receipt)
            elif name == "toolchain":
                write(path, toolchain)
            elif name == "compiled_dependencies":
                write(path, {"ImportPath": "synthetic/no-cgo", "GoFiles": ["fixture.go"]})
            elif name == "go_env":
                effective = process_environment(config["environment"])
                write(path, {**{key: effective[key] for key in ("GOROOT", "GOFLAGS", "GOWORK", "GOCACHE", "GOMODCACHE", "GOENV", "GOTOOLCHAIN", "GOPATH", "CGO_ENABLED")}, "GOENV": "", "GOOS": "linux", "GOARCH": "amd64"})
            else:
                path.write_text("synthetic-refusal-input " + name + "\n")
            retained[name] = {"path": path.name, "sha256": sha(path)}
            artifacts[name] = {"path": "/synthetic/build/" + name, "sha256": sha(path)}
        write(original / (variant + "-build-artifacts.json"), retained)
        receipt = original / (variant + "-build-receipt.json")
        write(receipt, {"binary_sha256": declaration["binary_sha256"], "source_tree_sha256": declaration["source_tree_sha256"],
                        "environment": config["environment"], "effective_process_environment": process_environment(config["environment"]),
                        "go_version": config["go_version"], "go_binary_sha256": config["go_binary_sha256"], "toolchain_identity": config["toolchain_identity"],
                        "race": False, "build_tags": [], "artifacts": artifacts})
        declaration.update(build_receipt="/synthetic/" + variant + "-build/build-receipt.json", build_receipt_sha256=sha(receipt))
    write(original / "config.json", config)
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

    def damage_toolchain(packet):
        raw = packet / "baseline-toolchain.raw"
        value = json.loads(raw.read_text())
        value["executables"][1]["sha256"] = "6" * 64
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
        ("wrong-frozen-fixture", damage_fixture, "fixture differs from frozen source"),
        ("same-product", damage_same_product, "distinct production_commit"),
        ("rehashed-toolchain", damage_toolchain, "Go toolchain inventory mismatch"),
        ("rehashed-go-version", lambda p: damage_build_toolchain(p, "go_version", "different"), "toolchain mismatch"),
        ("rehashed-go-launcher", lambda p: damage_build_toolchain(p, "go_binary_sha256", "7" * 64), "toolchain mismatch"),
        ("rehashed-compiled-cgo", damage_compiled_cgo, "compiled Cgo inputs forbidden"),
    ]
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
