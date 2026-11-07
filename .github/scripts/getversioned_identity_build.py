"""Rebuild only the eight original benchmark executables; never execute them."""

import hashlib
import json
import os
import shutil
from pathlib import Path
import subprocess
import tempfile
import zipfile

from getversioned_original_archive import sha256_file

HARNESS = "TreeDB/mvcc/primary_generation_routing_v6_test.go"
RUNNER = "scripts/mvcc_primary_cost_gate.py"


def build(root, base, head, manifest, archive, output):
    root, output = Path(root).resolve(), Path(output).resolve()
    if subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip() != head:
        raise ValueError("identity-build checkout differs from event head")
    if subprocess.check_output(["git", "status", "--porcelain"], cwd=root, text=True).strip():
        raise ValueError("identity-build checkout is dirty")
    with zipfile.ZipFile(archive) as zf:
        binding = json.loads(zf.read("primary/bindings.json"))
        original_env = zf.read("environment.txt").decode("utf-8", errors="strict")
        if sha256_file(root / HARNESS) != binding["harness_sha256"]:
            raise ValueError("PRIMARY harness changed")
        if sha256_file(root / RUNNER) != binding["runner_sha256"]:
            raise ValueError("PRIMARY runner changed")
        go_version = subprocess.check_output(["go", "version"], text=True).strip()
        if go_version not in original_env.splitlines():
            raise ValueError("Go version differs from original recorded version")
    output.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, GOWORK="off")
    go_env = json.loads(subprocess.check_output(["go", "env", "-json", "CC", "CXX", "CGO_ENABLED", "GOOS", "GOARCH"], text=True, env=env))
    compiler = shutil.which(go_env["CC"].split()[0])
    if compiler is None:
        raise ValueError("compiler identity unavailable")
    compiler_identity = {"path": str(Path(compiler).resolve()), "sha256": sha256_file(compiler)}
    actual = {}
    with tempfile.TemporaryDirectory(prefix="identity-build-", dir=output) as temporary:
        temp = Path(temporary)
        baseline = temp / "baseline"
        subprocess.run(["git", "worktree", "add", "--detach", str(baseline), base], cwd=root, check=True, timeout=120)
        try:
            primary_tmp = temp / "tmp"
            primary_tmp.mkdir()
            primary_env = dict(env, GOMAXPROCS="1", TMPDIR=str(primary_tmp))
            overlay = temp / "overlay.json"
            overlay.write_text(json.dumps({"Replace": {str(baseline / HARNESS): str(root / HARNESS)}}), encoding="utf-8")
            for revision, source in (("baseline", baseline), ("candidate", root)):
                for package in ("db", "caching", "treedb"):
                    path = temp / f"{revision}-{package}.test"
                    package_path = "./TreeDB" if package == "treedb" else f"./TreeDB/{package}"
                    subprocess.run(["go", "test", "-c", "-trimpath", "-buildvcs=false", "-o", str(path), package_path], cwd=source, env=env, check=True, timeout=600)
                    name = path.name
                    actual[name] = sha256_file(path)
                    if actual[name] != manifest["raw_elf_sha256"][name]:
                        raise ValueError("raw identity mismatch: " + name)
                path = temp / f"{revision}-primary.test"
                argv = ["go", "test", "-c", "-p=1", "-tags=treedb_test", "-trimpath", "-buildvcs=false"]
                if revision == "baseline":
                    argv += ["-overlay", str(overlay)]
                subprocess.run(argv + ["-o", str(path), "./TreeDB/mvcc"], cwd=source, env=primary_env, check=True, timeout=600)
                actual[path.name] = sha256_file(path)
                if actual[path.name] != manifest[f"primary_{revision}_elf_sha256"]:
                    raise ValueError("PRIMARY identity mismatch: " + revision)
        finally:
            subprocess.run(["git", "worktree", "remove", "--force", str(baseline)], cwd=root, check=True, timeout=120)
    return {"go_version": go_version, "go_env": go_env, "compiler": compiler_identity, "executables": actual,
            "harness_sha256": binding["harness_sha256"], "runner_sha256": binding["runner_sha256"]}
