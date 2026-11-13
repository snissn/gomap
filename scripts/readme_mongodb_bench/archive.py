#!/usr/bin/env python3
"""Package retained evidence; exclude databases, binaries, sources and caches."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import tarfile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root, out = args.root.resolve(), args.output.resolve()
    out.mkdir(parents=True, exist_ok=True)
    files = []
    for dirname in ('runs', 'recipe', 'recipe-history', 'build', 'setup', 'preflight', 'lifecycle-controls', 'campaign', 'derived'):
        files.extend(p for p in (root/dirname).rglob('*') if p.is_file() and '__pycache__' not in p.parts)
    files.extend(p for p in root.iterdir() if p.is_file() and p.suffix in ('.json','.md','.txt')
                 and not p.name.startswith('issue-') and not p.name.startswith('final-'))
    if (root/'issue-draft.md').exists():
        files.append(root/'issue-draft.md')
    files = sorted(set(files), key=lambda p:str(p.relative_to(root)))
    index = []
    archive = out/'raw.tar.gz'
    with archive.open('wb') as raw:
        with gzip.GzipFile(fileobj=raw, mode='wb', filename='', mtime=0, compresslevel=6) as gz:
            with tarfile.open(fileobj=gz, mode='w|', format=tarfile.PAX_FORMAT) as tar:
                for path in files:
                    if path.is_symlink():
                        raise ValueError('symlink in evidence: '+str(path))
                    rel = str(path.relative_to(root))
                    content = path.read_bytes()
                    index.append(dict(path=rel,bytes=len(content),sha256=hashlib.sha256(content).hexdigest()))
                    info = tarfile.TarInfo(rel)
                    info.size = len(content)
                    info.mtime = 0
                    info.mode = 0o644
                    with path.open('rb') as handle:
                        tar.addfile(info,handle)
    value = dict(schema_version=1,archive=dict(filename=archive.name,bytes=archive.stat().st_size,
                 sha256=hashlib.sha256(archive.read_bytes()).hexdigest()),files=index,
                 excluded=['db','bin','go-ycsb-source','__pycache__','core dumps','shared caches'])
    (out/'raw-index.json').write_text(json.dumps(value,indent=2)+'\n')
    for src,name in ((root/'freeze.json','freeze.json'),(root/'derived/summary.json','summary.json'),
                     (root/'recipe/census.go','census.go.txt')):
        shutil.copyfile(src,out/name)
    for name in ('aggregate.py','verify_archive.py'):
        shutil.copyfile(root/'recipe'/name,out/name)
    print(json.dumps(dict(files=len(files),bytes=value['archive']['bytes'],sha256=value['archive']['sha256'])))


if __name__ == '__main__':
    main()
