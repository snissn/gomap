#!/usr/bin/env python3
"""Capture exact committed runtime and actual harness bytes, before and after."""
import hashlib
import json
import pathlib
import subprocess


def git(*args):
    return subprocess.check_output(['git', *args], text=True).strip()


def source_identity():
    paths = ['cmd/collection_workload_bench/main.go',
             *sorted(str(p) for p in pathlib.Path('cmd/collection_workload_bench').glob('r1*.go')
                     if not p.name.endswith('_test.go')),
             'scripts/r1_collection_capture.sh', 'scripts/r1_collection_summary.py',
             'scripts/r1_collection_source.py']
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.encode() + b'\0')
        digest.update(pathlib.Path(path).read_bytes())
        digest.update(b'\0')
    # Local compiled runtime inputs; tests, docs and retained artifacts cannot
    # invalidate identical runtime evidence. External modules bind through go.sum
    # and buildinfo; compiler/CGO inputs bind in capture metadata.
    blobs = {}
    for line in git('ls-tree', '-r', 'HEAD').splitlines():
        metadata, path = line.split('\t', 1)
        relevant = path in ('go.mod', 'go.sum') or (
            path.startswith(('TreeDB/', 'cmd/internal/treedbstats/'))
            and pathlib.Path(path).suffix in ('.go', '.s', '.S', '.c', '.h', '.syso')
            and not path.endswith('_test.go'))
        if relevant:
            blobs[path] = metadata.split()[2]
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
