"""Validate compiled-module identity from real metadata and damaged copies.

This offline check does not compile, contact a network or generate timings.
"""
import argparse
import copy
import json
import os
from pathlib import Path
import subprocess
from unittest.mock import patch

from build import compiled_modules, objects, canonical_modules
from protocol import process_environment, sha, write, selected_inputs, build_inputs, digest, harness_manifest, C3_PACKAGE, C3_PRODUCT, C3_HARNESS_FILES, HARNESS_FILES, HARNESS_PACKAGES, git_object_id

def environment_smoke(out):
    """Exercise the actual environment helper and a child, without invoking Go."""
    controls = {"GOROOT": str(out / "toolchain"), "GOCACHE": str(out / "cache"),
                "GOMODCACHE": str(out / "gopath/pkg/mod"), "GOWORK": "off",
                "GOMAXPROCS": "4", "GOGC": "100", "GOMEMLIMIT": "off",
                "GOFLAGS": "", "TMPDIR": str(out / "temporary")}
    persisted = out / "poisoned-goenv"
    persisted.write_text("GOAMD64=v4\nGOEXPERIMENT=arenas\nCGO_ENABLED=1\nGOFLAGS=-race\n")
    poison = {"GOAMD64": "v4", "GOEXPERIMENT": "arenas", "CGO_ENABLED": "1",
              "GOENV": str(persisted), "GOTOOLCHAIN": "auto", "GOFLAGS": "-race",
              "GODEBUG": "asyncpreemptoff=1", "GOPATH": "/ambient/gopath",
              "CC": "/ambient/compiler", "CGO_CFLAGS": "-march=native",
              "PATH": "/ambient/bin", "HOME": "/ambient/home",
              "LD_PRELOAD": "/ambient/injection", "UNDECLARED_SENTINEL": "ambient"}
    expected = dict(controls, PATH=os.defpath, GOENV="off", GOTOOLCHAIN="local",
                    GOPATH=str(out / "gopath"), LC_ALL="C", CGO_ENABLED="0", GOAMD64="v1", GOEXPERIMENT="")
    argv = ["/usr/bin/env"]
    with patch.dict(os.environ, poison, clear=True):
        env = process_environment(controls)
        assert env == expected
        with (out / "environment-child.stdout").open("wb") as stdout, (out / "environment-child.stderr").open("wb") as stderr:
            child = subprocess.run(argv, env=env, stdout=stdout, stderr=stderr)
    assert child.returncode == 0 and not (out / "environment-child.stderr").read_bytes()
    observed = dict(line.split("=", 1) for line in (out / "environment-child.stdout").read_text().splitlines())
    assert observed == expected
    checks = [{"label": "ambient-poison-and-persisted-goenv-child", "passed": True,
               "scope": "actual helper and env executable child; actual Go defaults require ordinary runtime smoke"}]
    for label, key, value in (("undeclared-goamd64", "GOAMD64", "v4"),
                              ("declared-cgo-override", "CGO_ENABLED", "1"),
                              ("declared-goenv-override", "GOENV", str(persisted)),
                              ("non-string-control", "GOGC", 100),
                              ("relative-module-cache", "GOMODCACHE", "gopath/pkg/mod"),
                              ("root-gopath", "GOMODCACHE", "/pkg/mod")):
        damaged = dict(controls)
        damaged[key] = value
        try:
            process_environment(damaged)
        except ValueError as error:
            checks.append({"label": label, "refused": True, "reason": str(error)})
        else:
            raise AssertionError("accepted damaged process controls: " + label)
    write(out / "environment-construction.json", {"controls": controls, "poisoned_ambient": poison,
          "effective_process_environment": expected, "command": argv, "exit_code": child.returncode,
          "stdout_sha256": sha(out / "environment-child.stdout"), "stderr_sha256": sha(out / "environment-child.stderr"),
          "checks": checks})
    return checks

def inputs_smoke(out):
    """Actual finite files prove stability/equality without executing Go."""
    base = out / "selected-inputs"
    source, goroot, module, cache = (base / name for name in ("source", "go", "module", "cache"))
    for directory in (source, goroot / "src/standard", module, cache):
        directory.mkdir(parents=True)
    for path in (source / "fixture.go", source / "fixture.s", source / "fixture.txt", source / "go.mod", source / "go.sum", goroot / "src/standard/standard.go", module / "external.go", module / "go.mod"):
        path.write_text("selected input " + path.name + "\n")
        path.chmod(0o644)
    packages = [{"ImportPath": "product", "Dir": str(source), "GoFiles": ["fixture.go"],
                 "SFiles": ["fixture.s"], "EmbedFiles": ["fixture.txt"]},
                {"ImportPath": "standard", "Standard": True, "Dir": str(goroot / "src/standard"), "GoFiles": ["standard.go"]},
                {"ImportPath": "external", "Dir": str(module), "GoFiles": ["external.go"],
                 "Module": {"Path": "example.invalid/external", "Version": "v1.0.0", "Sum": "same checksum", "GoModSum": "same mod checksum", "Dir": str(module), "GoMod": str(module / "go.mod")}}]
    env = {"GOROOT": str(goroot), "GOCACHE": str(cache)}
    def inventory():
        return {"files": [{"path": p.relative_to(source).as_posix(), "sha256": sha(p),
                           "bytes": p.stat().st_size,
                           "git_mode": "100755" if p.stat().st_mode & 0o111 else "100644",
                           "git_blob": git_object_id("blob", p.read_bytes(), "sha1")}
                          for p in sorted(source.rglob("*")) if p.is_file()],
                "original_manifest": {"git_object_format": "sha1"}}
    before, generated = selected_inputs(packages, source, env)
    authority = inventory()
    def receipt(inputs):
        external = {key: {k: value[k] for k in ("sha256", "bytes", "mode")} for key, value in inputs.items() if not key.startswith("REPO/")}
        value = {"environment": env, "external_input_identity": digest(external), "artifacts": {}}
        for field, name in (("compiled_inputs_before_sha256", "compiled_inputs_before"), ("compiled_input_closure_sha256", "compiled_input_closure"), ("generated_nonpersistent_inputs_sha256", "generated_nonpersistent_inputs")):
            value[field] = digest(generated if name == "generated_nonpersistent_inputs" else inputs)
            value["artifacts"][name] = {"sha256": value[field]}
        return value
    frozen = receipt(before)
    build_inputs(frozen, frozen, packages, before, before, generated, str(source), authority)
    checks = [{"label": "actual-selected-files-stable", "passed": True}]
    def refuse(label, inputs, ident, expected):
        try:
            build_inputs(receipt(inputs), frozen, packages, inputs, inputs, generated, str(source), ident)
        except ValueError as error:
            assert expected in str(error), (label, str(error))
            checks.append({"label": label, "refused": str(error)})
        else:
            raise AssertionError("repository changed input accepted: " + label)
    for label, value in (("invalid-blob-proof", "!"), ("noncanonical-blob-proof", "YR==")):
        changed = copy.deepcopy(before)
        changed["REPO/fixture.go"]["git_blob_bytes"] = value
        refuse(label, changed, authority, "retained repository Git blob proof")
    changed = copy.deepcopy(before)
    changed["REPO/fixture.go"].pop("git_blob_bytes")
    refuse("missing-blob-proof", changed, authority, "missing retained repository Git blob proof")
    changed = copy.deepcopy(before)
    changed["REPO/fixture.go"]["extra_blob_proof"] = changed["REPO/fixture.go"]["git_blob_bytes"]
    refuse("ambiguous-blob-proof", changed, authority, "selected persistent input custody mismatch")
    for path in ("fixture.go", "fixture.s", "fixture.txt", "go.mod", "go.sum"):
        for field, replacement in (("sha256", "0" * 64), ("bytes", before["REPO/" + path]["bytes"] + 1),
                                   ("mode", 0o755), ("mode", 0o600)):
            changed = copy.deepcopy(before)
            changed["REPO/" + path][field] = replacement
            refuse(path + "-" + field + "-" + str(replacement), changed, authority,
                   "repository compiler input differs from Git source authority: " + path)
        absent = copy.deepcopy(authority)
        absent["files"] = [f for f in absent["files"] if f["path"] != path]
        refuse(path + "-missing-authority", before, absent,
               "repository compiler input missing from Git source authority: " + path)
        missing = copy.deepcopy(before)
        del missing["REPO/" + path]
        refuse(path + "-missing-selected-input", missing, authority, "selected persistent input closure mismatch")
    refuse("missing-source-authority", before, None, "repository compiler source authority missing")
    duplicate = copy.deepcopy(authority)
    duplicate["files"].append(copy.deepcopy(duplicate["files"][0]))
    refuse("ambiguous-source-authority", before, duplicate, "duplicate Git source authority paths")
    for field in ("sha256", "bytes", "git_mode"):
        incomplete = copy.deepcopy(authority)
        next(f for f in incomplete["files"] if f["path"] == "fixture.go").pop(field)
        refuse("missing-authority-" + field, before, incomplete, "invalid Git compiler input authority: fixture.go")
    for owner, path in (("GOROOT", goroot / "src/standard/standard.go"), ("module", module / "external.go"), ("module-metadata", module / "go.mod")):
        original = path.read_bytes()
        for mutation in ("bytes", "mode", "missing"):
            if mutation == "bytes": path.write_bytes(original + b"changed")
            elif mutation == "mode": path.chmod(0o600)
            else: path.unlink()
            try:
                changed, ledger = selected_inputs(packages, source, env)
                build_inputs(receipt(changed), frozen, packages, changed, changed, ledger, str(source), authority)
            except ValueError as error:
                checks.append({"label": owner + "-" + mutation, "refused": str(error)})
            else: raise AssertionError("external changed input accepted")
            path.write_bytes(original); path.chmod(0o644)
    executable = source / "fixture.go"
    executable.chmod(0o755)
    executable_inputs, ledger = selected_inputs(packages, source, env)
    build_inputs(receipt(executable_inputs), frozen, packages, executable_inputs, executable_inputs,
                 ledger, str(source), inventory())
    checks.append({"label": "matching-git-executable-mode-allowed", "passed": True})
    executable.chmod(0o644)
    (source / "fixture.go").write_text("candidate product change\n")
    candidate, ledger = selected_inputs(packages, source, env)
    candidate_authority = inventory()
    build_inputs(receipt(candidate), frozen, packages, candidate, candidate, ledger, str(source), candidate_authority)
    checks.append({"label": "repository-product-delta-allowed", "passed": True})
    try: build_inputs(frozen, frozen, packages, before, candidate, generated, str(source), candidate_authority)
    except ValueError as error: checks.append({"label": "persistent-build-drift", "refused": str(error)})
    else: raise AssertionError("build drift accepted")
    missing = copy.deepcopy(packages); missing[0]["HFiles"] = ["missing.h"]
    try: selected_inputs(missing, source, env)
    except ValueError as error: checks.append({"label": "ordinary-missing-is-not-generated", "refused": str(error)})
    else: raise AssertionError("ordinary missing input exempted")
    relocated = copy.deepcopy(candidate)
    relocated_packages = json.loads(json.dumps(packages).replace(str(base), str(base) + "-relocated"))
    relocated_env = json.loads(json.dumps(env).replace(str(base), str(base) + "-relocated"))
    for value in relocated.values(): value["path"] = value["path"].replace(str(base), str(base) + "-relocated")
    relocated_build = receipt(relocated); relocated_build["environment"] = relocated_env
    build_inputs(relocated_build, frozen, relocated_packages, relocated, relocated, [], str(source).replace(str(base), str(base) + "-relocated"), candidate_authority)
    checks.append({"label": "normalized-external-relocation-allowed", "passed": True})
    write(out / "selected-input-construction.json", {"scope": "actual finite selected-input helper checks; no Go execution or performance claim", "checks": checks})
    return checks

def harness_smoke(out, suite="c3"):
    """Complete selected harness authority; ordinary product test deltas stay free."""
    root = out / (suite + "-harness-inputs")
    harness_files = HARNESS_FILES[suite]
    package = HARNESS_PACKAGES[suite]
    directory = package.removeprefix("github.com/snissn/gomap/")
    source, goroot, cache = root / "source", root / "go", root / "cache"
    names = ["TreeDB/mvcc/product.go", "go.mod", "go.sum", *harness_files]
    for name in names:
        path = source / name; path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("actual finite input " + name + "\n"); path.chmod(0o644)
    std = goroot / "src/standard/standard.go"; std.parent.mkdir(parents=True); std.write_text("standard\n"); std.chmod(0o644)
    env = {"GOROOT": str(goroot), "GOCACHE": str(cache)}
    packages = [{"ImportPath": C3_PRODUCT, "Dir": str(source / "TreeDB/mvcc"), "GoFiles": ["product.go"], "Deps": ["standard"]},
                {"ImportPath": "standard", "Standard": True, "Dir": str(std.parent), "GoFiles": [std.name]},
                {"ImportPath": package + " [" + package + ".test]", "ForTest": package,
                 "Dir": str(source / directory), "GoFiles": [Path(n).name for n in harness_files if n.startswith(directory + "/")]},
                {"ImportPath": "github.com/snissn/gomap/TreeDB/internal/cowbench", "Dir": str(source / "TreeDB/internal/cowbench"), "GoFiles": ["admission.go"]}]
    def inventory():
        return {"files": [{"path": p.relative_to(source).as_posix(), "sha256": sha(p),
                           "bytes": p.stat().st_size,
                           "git_mode": "100755" if p.stat().st_mode & 0o111 else "100644",
                           "git_blob": git_object_id("blob", p.read_bytes(), "sha1")}
                          for p in sorted(source.rglob("*")) if p.is_file()],
                "original_manifest": {"git_object_format": "sha1"}}
    def closure(): return selected_inputs(packages, source, env)[0]
    original = harness_manifest(packages, closure(), inventory(), str(source), env, suite)
    checks = [{"label": "complete-harness-positive", "passed": True}]
    def refuse(label, operation, expected):
        try: operation()
        except ValueError as error:
            assert expected in str(error), (label, str(error))
            checks.append({"label": label, "refused": str(error)})
        else: raise AssertionError("accepted " + label)
    def frozen_gate(retained=None, authority=None):
        inputs, generated = selected_inputs(packages, source, env)
        observed = harness_manifest(packages, inputs, inventory(), str(source), env, suite)
        external = {key: {k: value[k] for k in ("sha256", "bytes", "mode")}
                    for key, value in inputs.items() if not key.startswith("REPO/")}
        build = {"environment": env, "suite": suite, "harness_input_identity": digest(observed),
                 "external_input_identity": digest(external), "artifacts": {}}
        retained = inputs if retained is None else retained
        for field, name in (("compiled_inputs_before_sha256", "compiled_inputs_before"),
                            ("compiled_input_closure_sha256", "compiled_input_closure"),
                            ("generated_nonpersistent_inputs_sha256", "generated_nonpersistent_inputs")):
            build[field] = digest(generated if name == "generated_nonpersistent_inputs" else retained)
            build["artifacts"][name] = {"sha256": build[field]}
        frozen = {"cases": [{"package": package}], "fixtures": original, "external_input_identity": digest(external)}
        build_inputs(build, frozen, packages, retained, retained, generated, str(source),
                     inventory() if authority is None else authority)
    frozen_gate()
    for path in ("TreeDB/mvcc/product.go", "go.mod", "go.sum"):
        for field, value in (("sha256", "0" * 64), ("bytes", 0), ("mode", 0o600)):
            retained = copy.deepcopy(closure())
            retained["REPO/" + path][field] = value
            refuse("rebound-" + path + "-" + field, lambda r=retained: frozen_gate(r),
                   "repository compiler input differs from Git source authority: " + path)
        absent = inventory()
        absent["files"] = [item for item in absent["files"] if item["path"] != path]
        refuse("unmanifested-" + path, lambda a=absent: frozen_gate(authority=a),
               "repository compiler input missing from Git source authority: " + path)
    for name in harness_files:
        path = source / name; original_bytes = path.read_bytes()
        path.write_bytes(original_bytes + b"changed\n")
        refuse("changed-" + Path(name).name, frozen_gate, "complete frozen")
        path.write_bytes(original_bytes)
    mode_path = source / harness_files[0]; mode_path.chmod(0o755)
    refuse("harness-mode-changed", frozen_gate, "complete frozen"); mode_path.chmod(0o644)
    omitted = copy.deepcopy(packages); omitted[2]["GoFiles"].remove("leak_test.go")
    inputs, _ = selected_inputs(omitted, source, env)
    refuse("testmain-not-selected", lambda: harness_manifest(omitted, inputs, inventory(), str(source), env, suite), "source/selected-input")
    helper = source / directory / "new_helper_test.go"; helper.write_text("new helper\n"); helper.chmod(0o644)
    refuse("extra-source-helper-not-selected", frozen_gate, "source/selected-input")
    packages[2]["GoFiles"].append(helper.name)
    refuse("extra-selected-helper-not-frozen", frozen_gate, "complete frozen")
    expanded = harness_manifest(packages, closure(), inventory(), str(source), env, suite)
    assert len(expanded) == len(original) + 1
    checks.append({"label": "new-helper-explicit-full-freeze-positive", "passed": True})
    packages[2]["GoFiles"].remove(helper.name); helper.unlink()
    foreign = source / "TreeDB/testsupport/helper.go"; foreign.parent.mkdir(parents=True); foreign.write_text("foreign helper\n")
    packages.append({"ImportPath": "testsupport", "Dir": str(foreign.parent), "GoFiles": [foreign.name]})
    refuse("test-only-helper-outside-authority", frozen_gate, "undeclared repository test harness")
    packages.pop(); foreign.unlink()
    (source / "TreeDB/mvcc/product.go").write_text("candidate production change\n")
    (source / "TreeDB/mvcc/candidate_regression_test.go").write_text("legitimate product regression test\n")
    assert harness_manifest(packages, closure(), inventory(), str(source), env, suite) == original
    frozen_gate()
    checks.append({"label": "product-code-and-regression-test-delta-positive", "passed": True})
    bad = copy.deepcopy(packages); bad[0].pop("Deps")
    refuse("missing-normal-product-graph", lambda: harness_manifest(bad, closure(), inventory(), str(source), env, suite), "dependency closure missing")
    write(out / (suite + "-harness-input-construction.json"), {"checks": checks, "scope": "finite actual files and pure metadata helpers; no Go"})
    return checks

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--compiled-packages", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=False)
    environment_checks = environment_smoke(args.out)
    input_checks = inputs_smoke(args.out)
    harness_checks = {suite: harness_smoke(args.out, suite) for suite in ("c3", "c4")}
    packages = objects(args.compiled_packages.read_text())
    mains = {p["Module"]["Dir"] for p in packages if p.get("Module", {}).get("Main")}
    assert len(mains) == 1
    source = Path(next(iter(mains))).resolve()
    modules = compiled_modules(packages, source)
    canonical = canonical_modules(modules, source)
    assert modules and len({m["Path"] for m in modules}) == len(modules)
    index = next(i for i, p in enumerate(packages) if p.get("Module", {}).get("Version") and not p["Module"].get("Replace"))
    def check(label, mutation, expected):
        damaged = copy.deepcopy(packages)
        mutation(damaged)
        try:
            compiled_modules(damaged, source)
        except (ValueError, KeyError) as error:
            assert expected in str(error), (label, str(error))
            return {"label": label, "refused": True, "reason": str(error)}
        raise AssertionError("accepted damaged metadata: " + label)
    def inconsistent(damaged):
        extra = copy.deepcopy(damaged[index])
        extra["Module"]["Version"] += "-changed"
        damaged.append(extra)
    def outside(damaged):
        damaged[index]["Module"]["Replace"] = {"Path": "synthetic/local", "Dir": "/synthetic-outside-frozen"}
    def version_missing(damaged):
        path = damaged[index]["Module"]["Path"]
        for package in damaged:
            module = package.get("Module", {})
            if module.get("Path") == path:
                for field in ("Version", "Sum", "GoModSum"):
                    module.pop(field, None)
    results = [
        check("missing-module", lambda p: p[index].pop("Module"), "lacks valid module"),
        check("module-error", lambda p: p[index]["Module"].update(Error={"Err": "synthetic"}), "lacks valid module"),
        check("missing-version-and-checksums", version_missing, "missing version"),
        check("missing-sum", lambda p: p[index]["Module"].pop("Sum"), "missing checksums"),
        check("missing-gomodsum", lambda p: p[index]["Module"].pop("GoModSum"), "missing checksums"),
        check("inconsistent-module", inconsistent, "inconsistent compiled module"),
        check("external-local-replacement", outside, "external local replacement"),
    ]
    write(args.out / "result.json", {"scope": "real compiled-module metadata, hermetic environment construction and synthetic damaged-copy refusal only; no Go build or timing acceptance",
        "input_sha256": sha(args.compiled_packages), "packages": len(packages), "compiled_modules": canonical,
        "checks": results, "environment_checks": environment_checks, "selected_input_checks": input_checks, "harness_checks": harness_checks,
        "script_sha256": sha(Path(__file__)), "build_script_sha256": sha(Path(__file__).parent / "build.py"),
        "protocol_script_sha256": sha(Path(__file__).parent / "protocol.py")})
    print(json.dumps({"compiled_modules": len(modules), "module_refusals": len(results),
                      "environment_checks": len(environment_checks), "selected_input_checks": len(input_checks), "harness_checks": {suite: len(checks) for suite, checks in harness_checks.items()}}))

if __name__ == "__main__":
    main()
