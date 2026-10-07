"""Build ordinary frozen MVCC test binaries, retaining actual provenance.

Root runs only after the implementation worker releases Linux185. This script
does not edit source; generated receipts/binary stay in a new output directory.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import time

from protocol import drift, identity, need, now, process_environment, sha, write, validate_go_environment, toolchain_inventory, validate_toolchain, validate_no_cgo, selected_inputs, digest, build_command, module_command, harness_manifest, repository_inputs

GIT_SOURCE_SCHEMA = "gomap-git-export-authority-v1"

def git_object_id(kind, raw, object_format):
    need(object_format in ("sha1", "sha256"), "unsupported Git object format")
    return hashlib.new(object_format, kind.encode() + b" " + str(len(raw)).encode() + b"\0" + raw).hexdigest()

def tree_inventory(tree, tree_objects, object_format):
    """Derive paths/modes/blob IDs from hash-verified raw Git tree objects."""
    width = 20 if object_format == "sha1" else 32
    files, used = {}, set()
    def walk(oid, prefix, ancestors):
        need(oid not in ancestors and oid in tree_objects, "missing/cyclic Git tree object")
        raw = base64.b64decode(tree_objects[oid], validate=True)
        need(git_object_id("tree", raw, object_format) == oid, "Git tree object hash mismatch")
        used.add(oid)
        position, names = 0, set()
        while position < len(raw):
            space = raw.find(b" ", position)
            nul = raw.find(b"\0", space + 1)
            need(space > position and nul > space + 1 and nul + 1 + width <= len(raw), "malformed Git tree object")
            mode = raw[position:space].decode("ascii")
            name = raw[space + 1:nul].decode("utf-8", "surrogateescape")
            need(name not in (".", "..", ".git") and "/" not in name and name not in names, "unsafe/duplicate Git tree path")
            names.add(name)
            child = raw[nul + 1:nul + 1 + width].hex()
            position = nul + 1 + width
            path = prefix + name
            if mode == "40000":
                walk(child, path + "/", ancestors | {oid})
            else:
                need(mode in ("100644", "100755"), "unsupported Git source mode: " + mode + " " + path)
                need(path not in files, "duplicate Git source path")
                files[path] = {"git_mode": mode, "git_blob": child}
    walk(tree, "", set())
    need(used == set(tree_objects), "extra Git tree objects")
    need(files, "empty Git source tree")
    return files

def verify_git_receipt(source, manifest, receipt):
    """Verify object authority offline; source additionally proves blob bytes/modes.

    With source=None, raw commit/tree proof binds the manifest inventory to Git
    blob IDs. SHA256/blob byte equivalence was checked at build admission and
    cannot be independently recomputed without the exported source bytes.
    """
    need(receipt["schema"] == GIT_SOURCE_SCHEMA, "unexpected Git source receipt schema")
    object_format = receipt["git_object_format"]
    width = {"sha1": 40, "sha256": 64}.get(object_format)
    need(width is not None, "unsupported Git object format")
    for key in ("git_head", "git_tree"):
        need(isinstance(receipt[key], str) and re.fullmatch(r"[0-9a-f]{%d}" % width, receipt[key]), "invalid exact Git " + key)
        need(manifest[key] == receipt[key], "Git manifest identity mismatch: " + key)
    need(manifest["git_object_format"] == object_format, "Git manifest object format mismatch")
    commit = base64.b64decode(receipt["commit_object"], validate=True)
    need(git_object_id("commit", commit, object_format) == receipt["git_head"], "Git commit object hash mismatch")
    need(commit.split(b"\n", 1)[0] == b"tree " + receipt["git_tree"].encode(), "Git commit does not own declared tree")
    inventory = tree_inventory(receipt["git_tree"], receipt["tree_objects"], object_format)
    files = manifest["files"]
    need(isinstance(files, list) and files == receipt["files"], "Git manifest/receipt files mismatch")
    need(len(files) == len(inventory) and len({item["path"] for item in files}) == len(files), "missing/extra/duplicate Git source files")
    for item in files:
        need(set(item) == {"path", "sha256", "bytes", "git_mode", "git_blob"}, "unexpected Git file manifest fields")
        need(item["path"] in inventory and all(item[key] == inventory[item["path"]][key] for key in ("git_mode", "git_blob")), "Git file path/mode/blob mismatch")
        need(isinstance(item["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", item["sha256"]), "invalid exported file SHA256")
        need(type(item["bytes"]) is int and item["bytes"] >= 0, "invalid exported file size")
    if source is not None:
        source = Path(source)
        need(source.is_dir() and not source.is_symlink(), "Git export source must be a real directory")
        need(not (source / ".git").exists() and not (source / ".git").is_symlink(), "source must be a detached export without .git metadata")
        expected_dirs = {"."}
        for path in inventory:
            expected_dirs.update(parent.as_posix() for parent in Path(path).parents)
        actual, directories = set(), {"."}
        for current, dirs, names in os.walk(source, followlinks=False):
            for name in dirs:
                path = Path(current) / name
                need(not path.is_symlink(), "symlink exported source directory: " + str(path))
                directories.add(path.relative_to(source).as_posix())
            for name in names:
                path = Path(current) / name
                need(stat.S_ISREG(path.lstat().st_mode), "nonregular/symlink exported source file: " + str(path))
                actual.add(path.relative_to(source).as_posix())
        need(actual == set(inventory), "missing/extra exported source files")
        need(directories == expected_dirs, "missing/extra exported source directories")
        for item in files:
            path = source / item["path"]
            actual_mode = "100755" if path.stat().st_mode & 0o111 else "100644"
            need(actual_mode == item["git_mode"],
                 "exported source mode mismatch: " + item["path"])
            raw = path.read_bytes()
            need(len(raw) == item["bytes"] and hashlib.sha256(raw).hexdigest() == item["sha256"], "exported source bytes mismatch: " + item["path"])
            need(git_object_id("blob", raw, object_format) == item["git_blob"], "exported source Git blob mismatch: " + item["path"])

def git_source_authority(source, repository, head, tree):
    """Read actual Git objects; no checkout, ref movement or source mutation."""
    source = Path(source)
    need(source.is_dir() and not source.is_symlink(), "Git export source must be a real directory")
    need(not (source / ".git").exists() and not (source / ".git").is_symlink(), "source must be a detached export without .git metadata")
    repository = Path(repository).resolve()
    need(repository.is_dir(), "Git object repository must exist")
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull, GIT_NO_LAZY_FETCH="1")
    prefix = ["git", "--no-replace-objects", "-C", str(repository)]
    commands = []
    def run(argv, input=None):
        process = subprocess.run(prefix + argv, input=input, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        commands.append({"command": prefix + argv, "exit_code": process.returncode,
                         "stdout_sha256": hashlib.sha256(process.stdout).hexdigest(),
                         "stderr": process.stderr.decode("utf-8", "replace")})
        need(process.returncode == 0, "Git object authority command failed: " + " ".join(argv))
        return process.stdout
    object_format = run(["rev-parse", "--show-object-format"]).decode().strip()
    width = {"sha1": 40, "sha256": 64}.get(object_format)
    need(width is not None, "unsupported Git repository object format")
    need(all(isinstance(oid, str) and re.fullmatch(r"[0-9a-f]{%d}" % width, oid) for oid in (head, tree)), "Git identity must be exact full object hashes")
    need(run(["cat-file", "-t", head]).strip() == b"commit", "Git head is not a commit object")
    commit = run(["cat-file", "commit", head])
    need(commit.split(b"\n", 1)[0] == b"tree " + tree.encode(), "declared Git tree is not commit tree")
    listing = run(["ls-tree", "-r", "-t", "-z", "--full-tree", tree])
    trees = {tree}
    for entry in listing.split(b"\0"):
        if entry:
            metadata, _ = entry.split(b"\t", 1)
            _, kind, oid = metadata.split(b" ")
            if kind == b"tree":
                trees.add(oid.decode())
    payload = run(["cat-file", "--batch"], ("\n".join(sorted(trees)) + "\n").encode())
    tree_objects, offset = {}, 0
    for oid in sorted(trees):
        newline = payload.find(b"\n", offset)
        need(newline >= offset, "missing Git batch header")
        header = payload[offset:newline].split()
        need(len(header) == 3 and header[0].decode() == oid and header[1] == b"tree", "wrong Git batch object")
        size = int(header[2]); start = newline + 1; end = start + size
        need(size >= 0 and end < len(payload) and payload[end:end+1] == b"\n", "truncated Git batch object")
        tree_objects[oid] = base64.b64encode(payload[start:end]).decode()
        offset = end + 1
    need(offset == len(payload), "extra Git batch output")
    inventory = tree_inventory(tree, tree_objects, object_format)
    files = []
    for path, record in sorted(inventory.items()):
        file = source / path
        need(file.is_file() and not file.is_symlink(), "missing/nonregular exported Git source: " + path)
        files.append(dict(path=path, sha256=sha(file), bytes=file.stat().st_size, **record))
    manifest = {"git_head": head, "git_tree": tree, "git_object_format": object_format, "files": files}
    receipt = dict(schema=GIT_SOURCE_SCHEMA, git_repository=str(repository), verified_at=now(),
                   commit_object=base64.b64encode(commit).decode(), tree_objects=tree_objects,
                   commands=commands, **manifest)
    verify_git_receipt(source, manifest, receipt)
    return manifest, receipt

def admit_tmpdir(controls, source, out):
    temporary = Path(controls["TMPDIR"])
    need(temporary.is_absolute() and temporary.is_dir() and not temporary.is_symlink() and temporary.resolve() == temporary,
         "TMPDIR must be an absolute existing real directory")
    need(temporary.stat().st_uid == os.getuid(), "TMPDIR must be owned by build user")
    need(not temporary.is_relative_to(source) and not temporary.is_relative_to(out), "TMPDIR must stay outside source/output")

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
    parser.add_argument("--git-repository", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--controls", type=Path, required=True)
    parser.add_argument("--suite", choices=("c3", "c4"), default="c3")
    args = parser.parse_args()
    need(not args.source.is_symlink(), "source must be a real exported directory")
    source, out = args.source.resolve(), args.out.resolve()
    need(not out.is_relative_to(source), "build output must stay outside frozen source")
    controls = json.loads(args.controls.read_text())
    required_controls = {"GOMAXPROCS", "GOWORK", "GOROOT", "GOGC", "GOMEMLIMIT", "GOFLAGS", "GOCACHE", "GOMODCACHE", "TMPDIR"}
    need(set(controls) == required_controls and all(isinstance(value, str) for value in controls.values()), "missing/extra/non-string build controls")
    need(controls["GOMAXPROCS"] == "4" and controls["GOWORK"] == "off" and controls["GOFLAGS"] == "", "unexpected build controls")
    env = process_environment(controls)
    admit_tmpdir(controls, source, out)
    manifest, git_receipt = git_source_authority(source, args.git_repository, args.git_head, args.git_tree)
    out.mkdir(parents=True, exist_ok=False)
    go = Path(controls["GOROOT"]) / "bin/go"
    write(out / "source-manifest.json", manifest)
    write(out / "git-source-authority.json", git_receipt)
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
        toolchain = toolchain_inventory(controls["GOROOT"])
        version = run("go-version", [str(go), "version"]).strip()
        go_env = json.loads(run("go-env", [str(go), "env", "-json"]))
        validate_go_environment(go_env, env)
        binary = out / ("cowbench-normal.test" if args.suite == "c3" else "cowsustained-normal.test")
        argv = build_command(go, binary, args.suite)
        before_packages = objects(run("compiled-dependencies-before", module_command(go, args.suite)))
        validate_no_cgo(before_packages)
        compiled_modules(before_packages, source)
        before, generated_before = selected_inputs(before_packages, source, controls)
        repository_inputs(before, ident)
        write(out / "compiled-inputs-before.json", before)
        run("build", argv)
        packages = objects(run("compiled-dependencies", module_command(go, args.suite)))
        validate_no_cgo(packages)
        modules = compiled_modules(packages, source)
        write(out / "module-graph.stdout", modules)
        write(out / "effective-module-graph.json", canonical_modules(modules, source))
        closure, generated_missing = selected_inputs(packages, source, controls)
        need(closure == before and generated_missing == generated_before, "selected persistent inputs drift during build")
        external = {key: {k: value[k] for k in ("sha256", "bytes", "mode")}
                    for key, value in closure.items() if not key.startswith("REPO/")}
        write(out / "compiled-input-closure.json", closure)
        write(out / "generated-nonpersistent-inputs.json", generated_missing)
        harness = harness_manifest(packages, closure, ident, str(source), controls, args.suite)
        run("binary-buildinfo", [str(go), "version", "-m", str(binary)])
        need(toolchain_inventory(controls["GOROOT"]) == toolchain, "Go toolchain drift during build")
        write(out / "toolchain.json", toolchain)
        bindings = {
            "toolchain": "toolchain.json",
            "git_source": "git-source-authority.json",
            "go_env": "go-env.stdout", "module_graph": "module-graph.stdout",
            "effective_module_graph": "effective-module-graph.json",
            "compiled_dependencies": "compiled-dependencies.stdout", "binary_buildinfo": "binary-buildinfo.stdout",
            "build_stdout": "build.stdout", "build_stderr": "build.stderr",
            "compiled_input_closure": "compiled-input-closure.json",
            "compiled_inputs_before": "compiled-inputs-before.json",
            "generated_nonpersistent_inputs": "generated-nonpersistent-inputs.json",
        }
        artifacts = {name: {"path": str(out / file), "sha256": sha(out / file)} for name, file in bindings.items()}
        changed = drift(source, ident)
        write(out / "source-after.json", {"at": now(), "drift": changed})
        need(not changed, "build/provenance mutated source")
        verify_git_receipt(source, manifest, git_receipt)
        write(out / "build-receipt.json", {"binary_sha256": sha(binary), "source_tree_sha256": ident["tree_sha256"],
            "environment": controls, "effective_process_environment": env,
            "suite": args.suite, "race": False, "build_tags": [], "go_version": version, "go_binary_sha256": toolchain["go_binary_sha256"],
            "toolchain_identity": validate_toolchain(toolchain),
            "command": argv, "exit_code": 0, "module_scope": "actual compiled package/test dependency Module records; all declared go.mod/go.sum retained separately",
            "module_producer_command": next(item["command"] for item in commands if item["name"] == "compiled-dependencies"), "effective_module_identity": sha(out / "effective-module-graph.json"),
            "harness_input_identity": digest(harness), "external_input_identity": digest(external), "compiled_inputs_before_sha256": sha(out / "compiled-inputs-before.json"),
            "artifacts": artifacts, "compiled_input_closure_sha256": sha(out / "compiled-input-closure.json"),
            "generated_nonpersistent_inputs_sha256": sha(out / "generated-nonpersistent-inputs.json")})
        print(json.dumps({"binary": str(binary), "sha256": sha(binary), "source_tree_sha256": ident["tree_sha256"]}), flush=True)
    except BaseException as error:
        write(out / "failure.json", {"at": now(), "type": type(error).__name__, "error": str(error)})
        raise

if __name__ == "__main__":
    main()
