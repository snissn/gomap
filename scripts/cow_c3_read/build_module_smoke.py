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
from protocol import process_environment, sha, write

def environment_smoke(out):
    """Exercise the actual environment helper and a child, without invoking Go."""
    controls = {"GOROOT": str(out / "toolchain"), "GOCACHE": str(out / "cache"),
                "GOMODCACHE": str(out / "gopath/pkg/mod"), "GOWORK": "off",
                "GOMAXPROCS": "4", "GOGC": "100", "GOMEMLIMIT": "off",
                "GOFLAGS": "", "TMPDIR": str(out / "temporary")}
    persisted = out / "poisoned-goenv"
    persisted.write_text("GOAMD64=v4\nGOEXPERIMENT=arenas\nCGO_ENABLED=0\nGOFLAGS=-race\n")
    poison = {"GOAMD64": "v4", "GOEXPERIMENT": "arenas", "CGO_ENABLED": "0",
              "GOENV": str(persisted), "GOTOOLCHAIN": "auto", "GOFLAGS": "-race",
              "GODEBUG": "asyncpreemptoff=1", "GOPATH": "/ambient/gopath",
              "CC": "/ambient/compiler", "CGO_CFLAGS": "-march=native",
              "PATH": "/ambient/bin", "HOME": "/ambient/home",
              "LD_PRELOAD": "/ambient/injection", "UNDECLARED_SENTINEL": "ambient"}
    expected = dict(controls, PATH=os.defpath, GOENV="off", GOTOOLCHAIN="local",
                    GOPATH=str(out / "gopath"), LC_ALL="C")
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

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--compiled-packages", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=False)
    environment_checks = environment_smoke(args.out)
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
        "checks": results, "environment_checks": environment_checks,
        "script_sha256": sha(Path(__file__)), "build_script_sha256": sha(Path(__file__).parent / "build.py"),
        "protocol_script_sha256": sha(Path(__file__).parent / "protocol.py")})
    print(json.dumps({"compiled_modules": len(modules), "module_refusals": len(results),
                      "environment_checks": len(environment_checks)}))

if __name__ == "__main__":
    main()
