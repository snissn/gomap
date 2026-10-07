#!/usr/bin/env python3
"""Capture committed source after verifying the actual local build inputs."""
import hashlib
import json
import os
import pathlib
import shlex
import subprocess

BUILD_FILE_FIELDS = ('GoFiles', 'CgoFiles', 'CFiles', 'CXXFiles', 'MFiles',
                     'HFiles', 'FFiles', 'SFiles', 'SwigFiles', 'SwigCXXFiles',
                     'SysoFiles', 'EmbedFiles')
RUNTIME_SUFFIXES = ('.go', '.s', '.S', '.c', '.h', '.syso')


def git(*args):
    return subprocess.check_output(['git', *args], text=True).strip()


def committed_files():
    files = {}
    for line in git('ls-tree', '-r', 'HEAD').splitlines():
        metadata, path = line.split('\t', 1)
        mode, kind, blob = metadata.split()
        files[path] = (mode, kind, blob)
    return files


def require_committed_bytes(path, files):
    if path not in files or files[path][0] not in ('100644', '100755'):
        raise ValueError('build input is not a committed regular file: ' + path)
    selected = pathlib.Path(path)
    if any(part.is_symlink() for part in (selected, *selected.parents)):
        raise ValueError('build input passes through a symlink: ' + path)
    actual = selected.read_bytes()
    expected = subprocess.check_output(['git', 'cat-file', 'blob', files[path][2]])
    if actual != expected:
        raise ValueError('working build input differs from committed bytes: ' + path)
    return actual


def compiled_local_files(go=None):
    root = pathlib.Path(git('rev-parse', '--show-toplevel')).resolve()
    selected_go = go or os.environ.get('R1_GO', 'go')
    build_environment = json.loads(subprocess.check_output(
        [selected_go, 'env', '-json', 'GOFLAGS', 'GOWORK'], text=True))
    if build_environment.get('GOWORK') not in ('', 'off'):
        raise ValueError('unbound active Go workspace')
    for flag in shlex.split(build_environment.get('GOFLAGS', '')):
        if flag.split('=', 1)[0] in ('-overlay', '-modfile'):
            raise ValueError('unbound Go build input substitution: ' + flag)
    raw = subprocess.check_output(
        [selected_go, 'list', '-deps', '-json',
         './cmd/collection_workload_bench'], text=True)
    decoder = json.JSONDecoder()
    pos = 0
    paths = set()
    while pos < len(raw):
        while pos < len(raw) and raw[pos].isspace():
            pos += 1
        if pos == len(raw):
            break
        package, pos = decoder.raw_decode(raw, pos)
        listed_directory = pathlib.Path(package['Dir'])
        directory = listed_directory.resolve()
        if listed_directory.is_relative_to(root) and not directory.is_relative_to(root):
            raise ValueError('local package escapes checkout: ' + str(listed_directory))
        replacement = package.get('Module', {}).get('Replace', {})
        if replacement and not replacement.get('Version'):
            replacement_dir = pathlib.Path(replacement.get('Dir', directory)).resolve()
            if not replacement_dir.is_relative_to(root):
                raise ValueError('unbound external local module replacement: ' + str(replacement_dir))
        if not directory.is_relative_to(root):
            continue
        for key in BUILD_FILE_FIELDS:
            for name in package.get(key, []):
                selected = listed_directory / name
                path = selected.resolve()
                if not path.is_relative_to(root):
                    raise ValueError('local build input escapes checkout: ' + str(path))
                if any(part.is_symlink() for part in (selected, *selected.parents)):
                    raise ValueError('local build input passes through a symlink: ' + str(selected))
                paths.add(path.relative_to(root).as_posix())
    if 'cmd/collection_workload_bench/main.go' not in paths:
        raise ValueError('benchmark entry point missing from compiled input inventory')
    return paths


def source_identity(go=None):
    files = committed_files()
    command = 'cmd/collection_workload_bench/'
    paths = sorted(path for path in files
                   if path.startswith(command) and path.endswith('.go')
                   and not path.endswith('_test.go'))
    paths += ['scripts/r1_collection_capture.sh', 'scripts/r1_collection_summary.py',
              'scripts/r1_collection_source.py']
    # Keep the conservative all-platform runtime inventory. Also discover the
    # actual local package inputs, including embeds and future local packages.
    blobs = {}
    for path, (_, _, blob) in files.items():
        relevant = path in ('go.mod', 'go.sum') or (
            path.startswith(('TreeDB/', 'cmd/internal/treedbstats/'))
            and pathlib.Path(path).suffix in RUNTIME_SUFFIXES
            and not path.endswith('_test.go'))
        if relevant:
            require_committed_bytes(path, files)
            blobs[path] = blob
    for path in sorted(compiled_local_files(go)):
        require_committed_bytes(path, files)
        if path not in paths:
            blobs[path] = files[path][2]
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.encode() + b'\0')
        digest.update(require_committed_bytes(path, files))
        digest.update(b'\0')
    runtime_digest = hashlib.sha256()
    for path, blob in sorted(blobs.items()):
        runtime_digest.update(path.encode() + b'\0' + blob.encode() + b'\0')
    return {'commit': git('rev-parse', 'HEAD'),
            'runtime_sha256': runtime_digest.hexdigest(),
            'runtime_blobs': blobs,
            'harness_sha256': digest.hexdigest(),
            'clean': not bool(git('status', '--porcelain', '--untracked-files=normal'))}


if __name__ == '__main__':
    print(json.dumps(source_identity(), indent=2))
