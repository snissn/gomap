#!/usr/bin/env python3
"""Verify the compact evidence archive against its complete file index."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import tarfile


def require(ok, message):
    if not ok:
        raise ValueError(message)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--directory', type=Path, required=True)
    args = parser.parse_args()
    directory = args.directory
    index = json.loads((directory/'raw-index.json').read_text())
    archive = directory/'raw.tar.gz'
    require(archive.stat().st_size == index['archive']['bytes'], 'archive size mismatch')
    require(hashlib.sha256(archive.read_bytes()).hexdigest() == index['archive']['sha256'], 'archive hash mismatch')
    expected = {f['path']:f for f in index['files']}
    require(len(expected) == len(index['files']), 'duplicate indexed path')
    observed = set()
    with tarfile.open(archive, 'r:gz') as tar:
        for member in tar:
            path = PurePosixPath(member.name)
            require(member.isfile() and not path.is_absolute() and '..' not in path.parts, 'unsafe archive entry')
            require(member.name in expected and member.name not in observed, 'extra or duplicate archive entry')
            observed.add(member.name)
            content = tar.extractfile(member).read()
            item = expected[member.name]
            require(len(content) == item['bytes'] == member.size, 'entry size mismatch')
            require(hashlib.sha256(content).hexdigest() == item['sha256'], 'entry hash mismatch')
    require(observed == set(expected), 'missing archive entry')
    print(json.dumps({'verified_files':len(observed),'archive_sha256':index['archive']['sha256']}))


if __name__ == '__main__':
    main()
