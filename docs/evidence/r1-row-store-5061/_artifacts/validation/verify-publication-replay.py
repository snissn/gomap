#!/usr/bin/env python3
"""Verify a finished publication stage and restore only indexed binary members."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import tarfile


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def require(ok, message):
    if not ok:
        raise ValueError(message)


def relative(value):
    parts = value.split('/')
    require(parts and all(re.fullmatch(r'[A-Za-z0-9_.-]+', part)
                         and part not in ('.', '..') for part in parts), 'unsafe path')
    require(not PurePosixPath(value).is_absolute(), 'absolute path')
    return value


def regular(path):
    require(stat.S_ISREG(path.lstat().st_mode), 'nonregular source: ' + str(path))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage', required=True)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    stage, out = Path(args.stage), Path(args.out)
    for path in (stage, out):
        require(path.is_absolute() and '..' not in path.parts, 'absolute canonical paths required')
        for parent in (path, *path.parents):
            require(not parent.is_symlink(), 'symlink path: ' + str(parent))
    require(stage.is_dir() and not out.exists(), 'missing stage or preexisting replay output')
    require(stage not in out.parents and out not in stage.parents, 'overlapping directories')
    ledger = stage / 'SHA256SUMS'
    regular(ledger)
    hashes = {}
    for line in ledger.read_text().splitlines():
        expected, name = line.split('  ', 1)
        relative(name)
        require(re.fullmatch(r'[0-9a-f]{64}', expected) and name not in hashes, 'bad checksum entry')
        path = stage / name
        for parent in path.parents:
            require(not parent.is_symlink(), 'symlink source parent')
        regular(path)
        require(digest(path) == expected, 'checksum mismatch: ' + name)
        hashes[name] = expected
    require('binary-overlay.tar.gz' in hashes and 'binary-member-index.json' in hashes,
            'missing indexed archive')
    records = json.loads((stage / 'binary-member-index.json').read_text())
    indexed = {}
    for record in records:
        require(set(record) == {'path', 'size', 'sha256'}, 'bad index record')
        name = relative(record['path'])
        require(name not in indexed and name not in hashes, 'duplicate or overlapping binary')
        require(type(record['size']) is int and record['size'] > 0
                and re.fullmatch(r'[0-9a-f]{64}', record['sha256']), 'bad binary size/hash')
        indexed[name] = record
    with tarfile.open(stage / 'binary-overlay.tar.gz', 'r:gz') as archive:
        members = archive.getmembers()
        require(len(members) == len(indexed) and {m.name for m in members} == set(indexed),
                'archive members differ from index')
        require(all(m.isfile() and not m.issym() and not m.islnk()
                    and m.size == indexed[m.name]['size'] for m in members), 'nonregular or wrong-sized archive member')
        out.mkdir(parents=True)
        for name in hashes:
            target = out / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(stage / name, target)
            require(digest(target) == hashes[name], 'replay text/copy hash mismatch')
        shutil.copyfile(ledger, out / 'SHA256SUMS')
        for member in members:
            target = out / member.name
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(member) as source, target.open('xb') as dest:
                shutil.copyfileobj(source, dest)
            require(target.stat().st_size == indexed[member.name]['size']
                    and digest(target) == indexed[member.name]['sha256'], 'restored binary mismatch')
            target.chmod(0o755)
    print(json.dumps({'verified_text_files': len(hashes), 'verified_binary_members': len(indexed),
                      'out': str(out), 'semantic_validator_replay': 'pending'}, sort_keys=True))


if __name__ == '__main__':
    main()
