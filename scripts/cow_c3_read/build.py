"""Build ordinary frozen MVCC test binaries, retaining actual provenance.

Root runs only after the implementation worker releases Linux185. This script
does not edit source; generated receipts/binary stay in a new output directory.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

from protocol import drift, identity, need, now, sha, write

def objects(raw):
    decoder, position = json.JSONDecoder(), 0
    result = []
    while position < len(raw):
        while position < len(raw) and raw[position].isspace():
            position += 1
        if position == len(raw):
            break
        item, position = decoder.raw_decode(raw, position)
        result.append(item)
    return result

def source_files(source):
    files = []
    for path in sorted(source.rglob("*")):
        need(not path.is_symlink(), "symlink source requires explicit policy: " + str(path))
        if path.is_file():
            files.append({"path": path.relative_to(source).as_posix(), "sha256": sha(path), "bytes": path.stat().st_size})
    return files

def canonical_modules(modules, source):
    # Retain every selected version/checksum and replacement. Source relocation
    # alone is normalized; local replacements include their complete file input.
    def record(module):
        result = {key: module[key] for key in ("Path", "Version", "Sum", "GoModSum", "Main", "GoVersion", "Indirect") if key in module}
        if module.get("Replace"):
            replacement = module["Replace"]
            result["Replace"] = record(replacement)
            if not replacement.get("Version"):
                directory = Path(replacement["Dir"]).resolve()
                need(directory.is_relative_to(source), "external local replacement requires separate frozen source: " + str(directory))
                result["Replace"]["source_relative_directory"] = directory.relative_to(source).as_posix()
                result["Replace"]["files"] = source_files(directory)
        return result
    return sorted((record(module) for module in modules), key=lambda item: item["Path"])

def compiled_modules(packages, source):
    # Only modules that Go actually selected for the compiled package closure.
    # Full go.mod/go.sum remain source-bound, including unused declarations.
    selected, mains = {}, set()
    for package in packages:
        if package.get("Standard"):
            continue
        module = package.get("Module")
        need(isinstance(module, dict) and module.get("Path") and not module.get("Error"),
             "non-standard compiled package lacks valid module: " + str(package.get("ImportPath")))
        if module.get("Main"):
            need(Path(module["Dir"]).resolve() == source, "compiled main module is outside frozen source")
            mains.add(module["Path"])
        def validate(item, main=False, replacement=False):
            need(item.get("Path") and not item.get("Error"), "invalid effective compiled module")
            if item.get("Replace"):
                validate(item["Replace"], replacement=True)
            elif not main:
                if replacement and not item.get("Version"):
                    need(item.get("Dir"), "local replacement lacks source directory")
                else:
                    need(item.get("Version"), "compiled external module missing version: " + item["Path"])
                    need(item.get("Sum") and item.get("GoModSum"), "compiled module missing checksums: " + item["Path"])
        validate(module, main=bool(module.get("Main")))
        canonical = canonical_modules([module], source)[0]
        old = selected.get(module["Path"])
        need(old is None or old[0] == canonical, "inconsistent compiled module: " + module["Path"])
        selected[module["Path"]] = (canonical, module)
    need(len(mains) == 1 and selected, "missing/extra compiled main module")
    return [selected[path][1] for path in sorted(selected)]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--git-head", required=True)
    parser.add_argument("--git-tree", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--controls", type=Path, required=True)
    args = parser.parse_args()
    source, out = args.source.resolve(), args.out.resolve()
    need(not out.is_relative_to(source), "build output must stay outside frozen source")
    controls = json.loads(args.controls.read_text())
    required_controls = {"GOMAXPROCS", "GOWORK", "GOROOT", "GOGC", "GOMEMLIMIT", "GOFLAGS", "GOCACHE", "GOMODCACHE"}
    need(set(controls) == required_controls and all(isinstance(value, str) for value in controls.values()), "missing/extra/non-string build controls")
    need(controls["GOMAXPROCS"] == "4" and controls["GOWORK"] == "off" and controls["GOFLAGS"] == "", "unexpected build controls")
    out.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    env.update(controls)
    go = Path(controls["GOROOT"]) / "bin/go"
    write(out / "source-manifest.json", {"git_head": args.git_head, "git_tree": args.git_tree, "files": source_files(source)})
    ident = identity(out / "source-manifest.json")
    write(out / "source-identity.json", ident)
    commands = []
    def run(name, argv):
        started = now()
        timer = time.monotonic()
        with (out / (name + ".stdout")).open("wb") as output, (out / (name + ".stderr")).open("wb") as errors:
            process = subprocess.run(argv, cwd=source, env=env, stdout=output, stderr=errors)
        commands.append({"name": name, "command": argv, "cwd": str(source), "started": started,
                         "completed": now(), "elapsed_seconds": time.monotonic() - timer, "exit_code": process.returncode})
        write(out / "commands.json", commands)
        need(process.returncode == 0, "failed provenance/build command " + name)
        return (out / (name + ".stdout")).read_text()
    try:
        version = run("go-version", [str(go), "version"]).strip()
        go_env = json.loads(run("go-env", [str(go), "env", "-json"]))
        for key in ("GOROOT", "GOFLAGS", "GOWORK", "GOCACHE", "GOMODCACHE"):
            need(go_env[key] == controls[key], "actual go env mismatch " + key)
        need(go_env["GOOS"] == "linux" and go_env["GOARCH"] == "amd64", "unexpected actual build platform")
        binary = out / "mvcc-normal.test"
        argv = [str(go), "test", "-c", "-o", str(binary), "./TreeDB/mvcc"]
        run("build", argv)
        packages = objects(run("compiled-dependencies", [str(go), "list", "-compiled", "-deps", "-test", "-json", "./TreeDB/mvcc"]))
        modules = compiled_modules(packages, source)
        write(out / "module-graph.stdout", modules)
        write(out / "effective-module-graph.json", canonical_modules(modules, source))
        closure, generated_missing = {}, []
        for package in packages:
            directory = Path(package.get("Dir", "."))
            for category in ("GoFiles", "CgoFiles", "CFiles", "HFiles", "SFiles", "SysoFiles", "EmbedFiles", "CompiledGoFiles"):
                for name in package.get(category, []):
                    path = directory / name
                    if path.is_file():
                        closure[str(path)] = {"sha256": sha(path), "bytes": path.stat().st_size}
                    else:
                        generated_missing.append({"package": package.get("ImportPath"), "category": category, "path": str(path)})
        for name in ("go.mod", "go.sum"):
            path = source / name
            closure[str(path)] = {"sha256": sha(path), "bytes": path.stat().st_size}
        write(out / "compiled-input-closure.json", closure)
        write(out / "generated-nonpersistent-inputs.json", generated_missing)
        run("binary-buildinfo", [str(go), "version", "-m", str(binary)])
        bindings = {
            "go_env": "go-env.stdout", "module_graph": "module-graph.stdout",
            "effective_module_graph": "effective-module-graph.json",
            "compiled_dependencies": "compiled-dependencies.stdout", "binary_buildinfo": "binary-buildinfo.stdout",
            "build_stdout": "build.stdout", "build_stderr": "build.stderr",
            "compiled_input_closure": "compiled-input-closure.json",
            "generated_nonpersistent_inputs": "generated-nonpersistent-inputs.json",
        }
        artifacts = {name: {"path": str(out / file), "sha256": sha(out / file)} for name, file in bindings.items()}
        changed = drift(source, ident)
        write(out / "source-after.json", {"at": now(), "drift": changed})
        need(not changed, "build/provenance mutated source")
        write(out / "build-receipt.json", {"binary_sha256": sha(binary), "source_tree_sha256": ident["tree_sha256"],
            "environment": controls, "race": False, "build_tags": [], "go_version": version, "go_binary_sha256": sha(go),
            "command": argv, "exit_code": 0, "module_scope": "actual compiled package/test dependency Module records; all declared go.mod/go.sum retained separately",
            "module_producer_command": commands[-2]["command"], "effective_module_identity": sha(out / "effective-module-graph.json"),
            "artifacts": artifacts, "compiled_input_closure_sha256": sha(out / "compiled-input-closure.json"),
            "generated_nonpersistent_inputs_sha256": sha(out / "generated-nonpersistent-inputs.json")})
        print(json.dumps({"binary": str(binary), "sha256": sha(binary), "source_tree_sha256": ident["tree_sha256"]}), flush=True)
    except BaseException as error:
        write(out / "failure.json", {"at": now(), "type": type(error).__name__, "error": str(error)})
        raise

if __name__ == "__main__":
    main()
