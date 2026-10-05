"""Retain exact completed churn metadata without copying databases or binaries."""
import hashlib,json,pathlib,tarfile
root=pathlib.Path('/mnt/fast4tb/quicksilver-space-memory-20261005')
files=[]
for variant in ('defaults','pack'):
    campaign=root/('churn-3m-1-'+variant)
    run=json.loads((campaign/('1-treedb-churn-'+variant)/'run.json').read_text())
    assert run['validated'] and run['rc']==0
    files.extend(p for p in campaign.rglob('*') if p.is_file())
    files.extend(root/name for name in ('churn-3m-1-'+variant+'-plan.json','churn-3m-1-'+variant+'-memory.jsonl','qualified-manifest-churn-'+variant+'.json'))
files.extend(p for p in (root/'receipts-churn-v3').rglob('*') if p.is_file())
files.extend(root/name for name in ('churn-landed-source-equality.json','churn-v3-source-inputs.json','freeze-churn-v3.py','churn-v3-build.py','observe.py','collect-churn.py'))
for p in files: assert p.is_file() and not p.is_symlink()
archive=root/'churn-final-pair-raw.tar.gz'
assert not archive.exists()
with tarfile.open(archive,'w:gz') as t:
    for p in sorted(set(files)):t.add(p,arcname=str(p.relative_to(root)),recursive=False)
print(json.dumps(dict(archive=str(archive),files=len(set(files)),bytes=archive.stat().st_size,sha256=hashlib.sha256(archive.read_bytes()).hexdigest())))
