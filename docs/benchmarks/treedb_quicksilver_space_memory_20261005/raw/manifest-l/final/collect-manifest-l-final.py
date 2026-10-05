"""Verify frozen L inputs/original identity and archive the completed final pass."""
import hashlib,json,pathlib,tarfile,time
r=pathlib.Path('/mnt/fast4tb/quicksilver-space-memory-20261005');sha=lambda p:hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest();s=json.loads((r/'receipts-manifest-l/source.json').read_text());b=json.loads((r/'receipts-manifest-l/build.json').read_text());n=json.loads((r/'receipts-manifest-l/native.json').read_text())
for p,h in s['files'].items():assert sha(pathlib.Path(s['source_path'])/p)==h
for p,h in s['all_compile_inputs'].items():assert sha(p)==h
for p,h in n['headers'].items():assert sha(p)==h
for p,v in n['libraries'].items():assert sha(p)==v['sha256']
for p,v in b['binaries'].items():assert sha(r/'bin'/p)==v['binary_sha256']
checks=[]
for variant in ('defaults','pack'):
 p=r/('churn-3m-'+variant+'-manifest-l-input/original-preserved.json');old=json.loads(p.read_text());d=pathlib.Path(old['original']);now=[[str(f.relative_to(d)),st.st_dev,st.st_ino,st.st_size,st.st_blocks,st.st_mtime_ns] for f in sorted(d.rglob('*')) if f.is_file() for st in [f.stat()]];assert now==old['after'];checks.append(dict(variant=variant,original_unchanged_after_fixed_maintenance=True,preserved_receipt_sha256=sha(p)))
receipt=r/'manifest-l-post-campaign-receipt.json';receipt.write_text(json.dumps(dict(created=time.time(),head=s['head'],source_files=len(s['files']),compile_inputs=len(s['all_compile_inputs']),headers=len(n['headers']),libraries=len(n['libraries']),all_frozen_inputs_unchanged=True,originals=checks),indent=2)+'\n')
d=r/'maintenance-churn-3m-defaults-manifest-l-exhaustive8192';files=[p for p in d.rglob('*') if p.is_file()]
for name in ('manifest-l-post-campaign-receipt.json','maintenance-churn-manifest-l-exhaustive.py','maintenance-churn-manifest-l-exhaustive-method.json','collect-manifest-l-final.py'):files.append(r/name)
a=r/'manifest-l-final-raw.tar.gz';assert not a.exists()
with tarfile.open(a,'w:gz') as t:
 for p in sorted(files):assert not p.is_symlink();t.add(p,arcname=str(p.relative_to(r)),recursive=False)
print(json.dumps(dict(files=len(files),bytes=a.stat().st_size,sha256=sha(a))))
