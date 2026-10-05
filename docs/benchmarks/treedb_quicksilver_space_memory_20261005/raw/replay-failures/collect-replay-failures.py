"""Archive replay methods and guards, keeping large changed DB blobs on Linux."""
import hashlib,json,pathlib,tarfile
r=pathlib.Path('/mnt/fast4tb/quicksilver-space-memory-20261005');files=[];excluded=[]
for directory in ('5004-replay-diagnostic','5004-replay-diagnostic-v2','replay-3m','replay-3m-no-vacuum'):
    for p in (r/directory).rglob('*'):
        if not p.is_file():continue
        assert not p.is_symlink()
        if p.suffix in ('.json','.jsonl','.py','.go','.md','.txt') or p.name in ('SHA256SUMS','stderr.log'):
            files.append(p)
        else:
            excluded.append(dict(path=str(p.relative_to(r)),bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest(),retained_native=True))
for directory in ('replay-3m','replay-3m-no-vacuum'):
    receipt=json.loads((r/directory/'1-baseline/run.json').read_text());assert receipt['rc']==1 and receipt['diagnostic_only']
    text=(r/directory/'1-baseline/stderr.log').read_text();assert 'resource identity changed' in text
names=('run-replay.py','run-replay-create-original.py','run-replay-pairs.py','run-replay-no-vacuum.py','run-replay-no-vacuum-pairs.py','prepare-replay-no-vacuum.py','replay-no-vacuum-method.json','build-replay.py','replay-3m-no-vacuum-memory.jsonl','collect-replay-failures.py')
files.extend(r/name for name in names)
inventory=r/'replay-native-retained-blobs.json';inventory.write_text(json.dumps(excluded,indent=2)+'\n');files.append(inventory)
for p in files:assert p.is_file()
a=r/'replay-failures-raw.tar.gz';assert not a.exists()
with tarfile.open(a,'w:gz') as t:
    for p in sorted(set(files)):t.add(p,arcname=str(p.relative_to(r)),recursive=False)
print(json.dumps(dict(archive=str(a),bytes=a.stat().st_size,sha256=hashlib.sha256(a.read_bytes()).hexdigest(),files=len(set(files)),native_retained_blobs=len(excluded))))
