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
from protocol import process_environment, sha, write, selected_inputs, build_inputs, digest

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
    for path in (source / "fixture.go", source / "go.mod", source / "go.sum", goroot / "src/standard/standard.go", module / "external.go", module / "go.mod"):
        path.write_text("selected input " + path.name + "\n")
        path.chmod(0o644)
    packages = [{"ImportPath": "product", "Dir": str(source), "GoFiles": ["fixture.go"]},
                {"ImportPath": "standard", "Standard": True, "Dir": str(goroot / "src/standard"), "GoFiles": ["standard.go"]},
                {"ImportPath": "external", "Dir": str(module), "GoFiles": ["external.go"],
                 "Module": {"Path": "example.invalid/external", "Version": "v1.0.0", "Sum": "same checksum", "GoModSum": "same mod checksum", "Dir": str(module), "GoMod": str(module / "go.mod")}}]
    env = {"GOROOT": str(goroot), "GOCACHE": str(cache)}
    before, generated = selected_inputs(packages, source, env)
    def receipt(inputs):
        external = {key: {k: value[k] for k in ("sha256", "bytes", "mode")} for key, value in inputs.items() if not key.startswith("REPO/")}
        value = {"environment": env, "external_input_identity": digest(external), "artifacts": {}}
        for field, name in (("compiled_inputs_before_sha256", "compiled_inputs_before"), ("compiled_input_closure_sha256", "compiled_input_closure"), ("generated_nonpersistent_inputs_sha256", "generated_nonpersistent_inputs")):
            value[field] = "1" * 64
            value["artifacts"][name] = {"sha256": value[field]}
        return value
    frozen = receipt(before)
    build_inputs(frozen, frozen, packages, before, before, generated, str(source))
    checks = [{"label": "actual-selected-files-stable", "passed": True}]
    for owner, path in (("GOROOT", goroot / "src/standard/standard.go"), ("module", module / "external.go"), ("module-metadata", module / "go.mod")):
        original = path.read_bytes()
        for mutation in ("bytes", "mode", "missing"):
            if mutation == "bytes": path.write_bytes(original + b"changed")
            elif mutation == "mode": path.chmod(0o600)
            else: path.unlink()
            try:
                changed, ledger = selected_inputs(packages, source, env)
                build_inputs(receipt(changed), frozen, packages, changed, changed, ledger, str(source))
            except ValueError as error:
                checks.append({"label": owner + "-" + mutation, "refused": str(error)})
            else: raise AssertionError("external changed input accepted")
            path.write_bytes(original); path.chmod(0o644)
    (source / "fixture.go").write_text("candidate product change\n")
    candidate, ledger = selected_inputs(packages, source, env)
    build_inputs(receipt(candidate), frozen, packages, candidate, candidate, ledger, str(source))
    checks.append({"label": "repository-product-delta-allowed", "passed": True})
    try: build_inputs(frozen, frozen, packages, before, candidate, generated, str(source))
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
    build_inputs(relocated_build, frozen, relocated_packages, relocated, relocated, [], str(source).replace(str(base), str(base) + "-relocated"))
    checks.append({"label": "normalized-external-relocation-allowed", "passed": True})
    write(out / "selected-input-construction.json", {"scope": "actual finite selected-input helper checks; no Go execution or performance claim", "checks": checks})
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
        "checks": results, "environment_checks": environment_checks, "selected_input_checks": input_checks,
        "script_sha256": sha(Path(__file__)), "build_script_sha256": sha(Path(__file__).parent / "build.py"),
        "protocol_script_sha256": sha(Path(__file__).parent / "protocol.py")})
    print(json.dumps({"compiled_modules": len(modules), "module_refusals": len(results),
                      "environment_checks": len(environment_checks), "selected_input_checks": len(input_checks)}))

if __name__ == "__main__":
    main()
