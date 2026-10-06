"""Validate compiled-module identity from real metadata and damaged copies.

This offline check does not compile, contact a network or generate timings.
"""
import argparse
import copy
import json
from pathlib import Path

from build import compiled_modules, objects, canonical_modules
from protocol import sha, write

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--compiled-packages", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
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
    write(args.out / "result.json", {"scope": "real compiled-module metadata and synthetic damaged-copy refusal only; no build or timing acceptance",
        "input_sha256": sha(args.compiled_packages), "packages": len(packages), "compiled_modules": canonical,
        "checks": results, "script_sha256": sha(Path(__file__)), "build_script_sha256": sha(Path(__file__).parent / "build.py")})
    print(json.dumps({"compiled_modules": len(modules), "refusals": len(results)}))

if __name__ == "__main__":
    main()
