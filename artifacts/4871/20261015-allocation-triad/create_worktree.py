#!/usr/bin/env python3
"""Create a native Git worktree, preferring tracked-file CoW with safe fallback."""

import argparse
import ctypes
import errno
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile


class SetupError(RuntimeError):
    pass


class OwnershipError(SetupError):
    pass


class StorageError(SetupError):
    pass


def brief_error(data):
    lines = data.decode(errors="replace").strip().splitlines()
    if len(lines) > 12:
        lines = lines[:8] + ["... diagnostics truncated ..."] + lines[-4:]
    return "\n".join(lines)[:3500]


def git(repo, *args, check=True):
    environment = dict(os.environ, GIT_OPTIONAL_LOCKS="0")
    for name in ("GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE", "GIT_PREFIX"):
        environment.pop(name, None)
    result = subprocess.run(
        ["git", "-C", os.fspath(repo), *args],
        env=environment,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if check and result.returncode:
        detail = brief_error(result.stderr or result.stdout)
        if any(word in result.stderr.lower() for word in
               (b"no space left on device", b"out of diskspace", b"disk quota exceeded")):
            raise StorageError("insufficient storage; retain incomplete setup: " + detail)
        raise SetupError(detail or "Git command failed: " + " ".join(args))
    return result


def head(repo):
    return git(repo, "rev-parse", "HEAD").stdout.strip().decode()


def common_dir(repo):
    return Path(os.fsdecode(git(
        repo, "rev-parse", "--path-format=absolute", "--git-common-dir"
    ).stdout.strip())).resolve()


def dirty(repo):
    return bool(git(repo, "status", "--porcelain=v1", "-z",
                    "--untracked-files=no", "--ignore-submodules=none").stdout)


def entries(repo, base):
    rows = git(repo, "ls-tree", "-r", "-z", base).stdout.split(b"\0")
    result = []
    for row in filter(None, rows):
        metadata, path = row.split(b"\t", 1)
        mode, kind, oid = metadata.split()
        result.append((mode.decode(), os.fsdecode(path)))
    return result


def check_case_collisions(parent, files):
    seen = {}
    collision = None
    for _, name in files:
        parts = Path(name).parts
        for end in range(1, len(parts) + 1):
            prefix = "/".join(parts[:end])
            prior = seen.setdefault(prefix.casefold(), prefix)
            if prior != prefix:
                collision = (prior, prefix)
                break
        if collision:
            break
    if collision:
        with tempfile.TemporaryDirectory(prefix=".cow-case-", dir=parent) as probe:
            lower = Path(probe) / "case-test"
            lower.touch()
            if (Path(probe) / "CASE-TEST").exists():
                raise SetupError(
                    "target filesystem cannot represent case-colliding tracked paths: "
                    + repr(collision) + "; use a case-sensitive filesystem"
                )


def seed_reason(repo, seed, target, base, files):
    if sys.platform != "darwin":
        return "native CoW backend not qualified on this platform"
    if any(mode == "160000" for mode, _ in files):
        return "submodules require the ordinary Git checkout path"
    try:
        if common_dir(seed) != common_dir(repo):
            return "seed belongs to another Git repository"
        if head(seed) != base:
            return "seed HEAD differs from the pinned base"
        if dirty(seed):
            return "seed has tracked changes"
        sparse = git(seed, "config", "--bool", "core.sparseCheckout", check=False)
        if sparse.stdout.strip() == b"true":
            return "sparse seed cannot populate a full checkout"
        if seed.stat().st_dev != target.parent.stat().st_dev:
            return "seed and target are on different filesystems"
    except StorageError:
        raise
    except (OSError, SetupError) as exc:
        return "seed unavailable: " + str(exc)
    return None


def clone_file(source, target):
    if sys.platform == "darwin":
        libc = ctypes.CDLL(None, use_errno=True)
        try:
            clone = libc.clonefile
        except AttributeError as exc:
            raise OSError(errno.ENOTSUP, "clonefile unavailable") from exc
        clone.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int)
        clone.restype = ctypes.c_int
        if clone(os.fsencode(source), os.fsencode(target), 1):  # CLONE_NOFOLLOW
            code = ctypes.get_errno()
            raise OSError(code, os.strerror(code), os.fspath(source))
    else:
        raise OSError(errno.ENOTSUP, "native CoW backend unavailable")


def check_owner(target, branch, base, identity):
    info = target.lstat()
    if not stat.S_ISDIR(info.st_mode) or (info.st_dev, info.st_ino) != identity:
        raise OwnershipError("destination was replaced during setup; retain it for inspection")
    if head(target) != base:
        raise OwnershipError("new worktree HEAD changed during setup; retain it for inspection")
    actual = git(target, "symbolic-ref", "--short", "HEAD").stdout.strip().decode()
    if actual != branch:
        raise OwnershipError("new worktree branch changed during setup; retain it for inspection")


def copy_tracked(seed, target, files):
    count = size = 0
    for mode, name in files:
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts or relative.parts[0].casefold() == ".git":
            raise SetupError("unsafe tracked pathname: " + repr(name))
        source, destination = seed / relative, target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        info = source.lstat()
        if mode == "120000" and stat.S_ISLNK(info.st_mode):
            os.symlink(os.readlink(source), destination)
        elif mode in ("100644", "100755") and stat.S_ISREG(info.st_mode):
            clone_file(source, destination)
            count += 1
            size += info.st_size
        else:
            raise SetupError("seed file type differs from the pinned tree: " + repr(name))
    return count, size


def validate(target, branch, base, identity):
    check_owner(target, branch, base, identity)
    if git(target, "status", "--porcelain=v1", "-z", "--untracked-files=all").stdout:
        raise SetupError("new checkout is not clean at the pinned base; retain it for inspection")


def create_worktree(repo, path, branch, base, seed=None, mode="auto"):
    repo, target = Path(repo).resolve(), Path(path).resolve()
    seed = Path(seed).resolve() if seed is not None else repo
    # lexists also rejects dangling symlinks. No existing destination is reused.
    if os.path.lexists(path) or target.exists():
        raise SetupError("destination already exists; refusing to overwrite: " + os.fspath(path))
    if not target.parent.is_dir():
        raise SetupError("destination parent must already exist: " + os.fspath(target.parent))
    if target.is_relative_to(common_dir(repo)):
        raise SetupError("destination is inside Git administration storage")
    checked = git(repo, "check-ref-format", "--branch", branch).stdout.strip().decode()
    if checked != branch:
        raise SetupError("use a literal new branch name")
    if git(repo, "show-ref", "--verify", "--quiet", "refs/heads/" + branch, check=False).returncode == 0:
        raise SetupError("branch already exists; refusing to reset it: " + branch)
    base = git(repo, "rev-parse", "--verify", "--end-of-options", base + "^{commit}").stdout.strip().decode()
    files = entries(repo, base)
    check_case_collisions(target.parent, files)
    reason = "ordinary Git explicitly selected" if mode == "git" else seed_reason(repo, seed, target, base, files)
    target.mkdir(mode=0o700)  # Atomically reserve the new, empty destination.
    identity = target.stat().st_dev, target.stat().st_ino
    args = ["worktree", "add"] + (["--no-checkout"] if reason is None else [])
    try:
        git(repo, *args, "-b", branch, os.fspath(target), base)
    except SetupError:
        if (target.stat().st_dev, target.stat().st_ino) == identity:
            try:
                target.rmdir()  # Only our still-empty reservation, never recursive deletion.
            except OSError:
                pass
        raise
    count = size = 0
    if reason is None:
        git(target, "read-tree", base)
        try:
            count, size = copy_tracked(seed, target, files)
            if head(seed) != base or dirty(seed):
                raise SetupError("seed changed during copying")
            check_owner(target, branch, base, identity)
            git(target, "update-index", "--refresh")
            validate(target, branch, base, identity)
        except (OSError, SetupError) as exc:
            if isinstance(exc, (OwnershipError, StorageError)):
                raise
            if isinstance(exc, OSError) and exc.errno in (errno.ENOSPC, errno.EDQUOT):
                raise StorageError(
                    "insufficient storage; retain incomplete setup instead of making a full copy"
                ) from exc
            check_owner(target, branch, base, identity)
            reason = "CoW setup unavailable or invalid: " + str(exc)
            # Only this freshly reserved worktree is written. Workers receive it
            # after success; these tracked paths were created solely by setup.
            git(target, "checkout-index", "--all", "--force")
            git(target, "update-index", "--refresh")
            count = size = 0
    validate(target, branch, base, identity)
    return {
        "mode": "git" if reason else "cow", "worktree": os.fspath(target),
        "branch": branch, "base_sha": base, "seed": os.fspath(seed),
        "fallback_reason": reason, "cloned_files": count, "cloned_bytes": size,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".")
    parser.add_argument("--path", required=True)
    parser.add_argument("--branch", required=True)
    parser.add_argument("--base", required=True, help="Pinned commit/ref; resolved once before setup")
    parser.add_argument("--seed", help="Quiescent clean worktree at the pinned base; defaults to repo")
    parser.add_argument("--mode", choices=("auto", "git"), default="auto")
    args = parser.parse_args()
    try:
        result = create_worktree(args.repo, args.path, args.branch, args.base, args.seed, args.mode)
    except (OSError, SetupError) as exc:
        print(json.dumps({"error": str(exc), "worktree": args.path}), file=sys.stderr)
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
