"""Small real-Git export admission smoke. No Go commands or performance claims."""
import argparse
import base64
import copy
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile

from build import admit_tmpdir, git_source_authority, verify_git_receipt
from protocol import write

def git(repository, *args):
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
    return subprocess.check_output(["git", "-C", str(repository), *args], env=env, stderr=subprocess.PIPE)

def fixture(root):
    """Return actual repository and detached archive, not a synthetic receipt."""
    repository, source = root / "repository", root / "export"
    repository.mkdir(); source.mkdir()
    git(repository, "init", "-q", "--object-format=sha1")
    (repository / "nested").mkdir()
    (repository / "nested" / "fixture.go").write_bytes(b"package fixture\n")
    (repository / "binary.dat").write_bytes(b"\0\xffretained\n")
    (repository / "run.sh").write_bytes(b"#!/bin/sh\nexit 0\n")
    (repository / "run.sh").chmod(0o755)
    git(repository, "add", ".")
    git(repository, "-c", "user.name=Git admission smoke", "-c", "user.email=smoke@example.invalid", "commit", "-qm", "fixture")
    head = git(repository, "rev-parse", "HEAD").decode().strip()
    tree = git(repository, "rev-parse", "HEAD^{tree}").decode().strip()
    archive = git(repository, "archive", "--format=tar", head)
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        # This archive contains only the locally constructed three-file fixture.
        # Explicit member checks also work with Python versions before filters.
        assert all((member.isfile() or member.isdir()) and not Path(member.name).is_absolute()
                   and ".." not in Path(member.name).parts for member in tar.getmembers())
        tar.extractall(source)
    return repository, source, head, tree

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    root = args.out.resolve()
    root.mkdir(parents=True, exist_ok=False)
    results = []
    def refusal(name, call):
        try:
            call()
        except (ValueError, KeyError, OSError) as error:
            results.append({"case": name, "status": "refused", "reason": str(error)})
        else:
            raise AssertionError("invalid Git source accepted: " + name)
    # Retain the actual object repository, detached sources and proof packet.
    try:
        repository, source, head, tree = fixture(root)
        assert not (source / ".git").exists()
        manifest, receipt = git_source_authority(source, repository, head, tree)
        write(root / "source-manifest.json", manifest)
        write(root / "git-source-authority.json", receipt)
        verify_git_receipt(source, manifest, receipt)
        verify_git_receipt(None, manifest, receipt)
        results.append({"case": "valid-detached-export-live-and-offline", "status": "accepted", "files": len(manifest["files"])})
        (repository / "nested" / "fixture.go").write_bytes(b"package newer\n")
        git(repository, "add", ".")
        git(repository, "-c", "user.name=Git admission smoke", "-c", "user.email=smoke@example.invalid", "commit", "-qm", "second")
        other = git(repository, "rev-parse", "HEAD").decode().strip()
        other_tree = git(repository, "rev-parse", "HEAD^{tree}").decode().strip()
        refusal("wrong-existing-commit", lambda: git_source_authority(source, repository, other, tree))
        refusal("wrong-existing-tree", lambda: git_source_authority(source, repository, head, other_tree))
        refusal("tree-used-as-commit", lambda: git_source_authority(source, repository, tree, tree))
        refusal("abbreviated-commit", lambda: git_source_authority(source, repository, head[:12], tree))
        refusal("sha256-hash-in-sha1-repository", lambda: git_source_authority(source, repository, head, "0" * 64))
        refusal("missing-commit-object", lambda: git_source_authority(source, repository, "0" * 40, tree))
        def changed_case(name, mutate):
            changed = root / name
            shutil.copytree(source, changed)
            mutate(changed)
            refusal(name, lambda: git_source_authority(changed, repository, head, tree))
        changed_case("wrong-source-bytes", lambda path: (path / "binary.dat").write_bytes(b"different"))
        changed_case("lost-executable-mode", lambda path: (path / "run.sh").chmod(0o644))
        changed_case("added-executable-mode", lambda path: (path / "binary.dat").chmod(0o755))
        changed_case("missing-source-file", lambda path: (path / "binary.dat").unlink())
        changed_case("extra-source-file", lambda path: (path / "extra").write_bytes(b"extra"))
        changed_case("extra-empty-directory", lambda path: (path / "extra").mkdir())
        changed_case("git-metadata-in-export", lambda path: (path / ".git").mkdir())
        def symlink_file(path):
            (path / "binary.dat").unlink()
            (path / "binary.dat").symlink_to(source / "binary.dat")
        changed_case("symlink-source-file", symlink_file)
        def symlink_directory(path):
            shutil.rmtree(path / "nested")
            (path / "nested").symlink_to(source / "nested", target_is_directory=True)
        changed_case("symlink-source-directory", symlink_directory)
        corrupted = copy.deepcopy(receipt)
        corrupted["commit_object"] = base64.b64encode(base64.b64decode(corrupted["commit_object"]) + b"bad").decode()
        refusal("corrupted-raw-commit-proof", lambda: verify_git_receipt(None, manifest, corrupted))
        corrupted_tree = copy.deepcopy(receipt)
        corrupted_tree["tree_objects"][tree] = base64.b64encode(b"bad").decode()
        refusal("corrupted-raw-tree-proof", lambda: verify_git_receipt(None, manifest, corrupted_tree))
        wrong_manifest = copy.deepcopy(manifest)
        wrong_receipt = copy.deepcopy(receipt)
        wrong_manifest["files"][0]["git_blob"] = "0" * 40
        wrong_receipt["files"] = wrong_manifest["files"]
        refusal("self-consistent-false-manifest-blob", lambda: verify_git_receipt(None, wrong_manifest, wrong_receipt))
        (repository / "linked").symlink_to("binary.dat")
        git(repository, "add", "linked")
        git(repository, "-c", "user.name=Git admission smoke", "-c", "user.email=smoke@example.invalid", "commit", "-qm", "unsupported symlink")
        linked_head = git(repository, "rev-parse", "HEAD").decode().strip()
        linked_tree = git(repository, "rev-parse", "HEAD^{tree}").decode().strip()
        refusal("git-tree-symlink-mode", lambda: git_source_authority(source, repository, linked_head, linked_tree))
        tmpdir = root / "persistent-tmp"
        tmpdir.mkdir()
        admit_tmpdir({"TMPDIR": str(tmpdir)}, source, root / "build-output")
        results.append({"case": "valid-owned-tmpdir", "status": "accepted"})
        refusal("relative-tmpdir", lambda: admit_tmpdir({"TMPDIR": "relative"}, source, root / "build-output"))
        refusal("source-tmpdir", lambda: admit_tmpdir({"TMPDIR": str(source)}, source, root / "build-output"))
        (root / "tmp-link").symlink_to(tmpdir, target_is_directory=True)
        refusal("symlink-tmpdir", lambda: admit_tmpdir({"TMPDIR": str(root / "tmp-link")}, source, root / "build-output"))
    except BaseException as error:
        write(root / "failure.json", {"type": type(error).__name__, "error": str(error), "completed_results": results})
        raise
    result = {"scope": "real Git export and TMPDIR admission only; no Go jobs or performance evidence", "results": results}
    write(root / "result.json", result)
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()
