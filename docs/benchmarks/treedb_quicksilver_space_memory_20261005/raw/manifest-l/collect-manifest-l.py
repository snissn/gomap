"""Archive completed L evidence; retained databases and binaries stay on Linux."""
import hashlib,json,pathlib,tarfile
r=pathlib.Path('/mnt/fast4tb/quicksilver-space-memory-20261005');files=[]
for name in ('receipts-manifest-l','manifest-l-preflight','manifest-l-native-tests','churn-3m-defaults-manifest-l-input','churn-3m-pack-manifest-l-input','maintenance-churn-3m-defaults-manifest-l-full8192','maintenance-churn-3m-pack-manifest-l-full8192'):
 for p in (r/name).rglob('*'):
  if p.is_file():
   assert not p.is_symlink();files.append(p)
for name in ('manifest-l-source-inputs.json','provisional-manifest-manifest-l.json','manifest-l-build.py','freeze-manifest-l.py','test-manifest-l.py','run-churn-manifest-l-copy.py','maintenance-churn-manifest-l-full.py','maintenance-churn-manifest-l-full-method.json','collect-manifest-l.py'):
 p=r/name;assert p.is_file();files.append(p)
a=r/'manifest-l-raw.tar.gz';assert not a.exists()
with tarfile.open(a,'w:gz') as t:
 for p in sorted(set(files)):t.add(p,arcname=str(p.relative_to(r)),recursive=False)
print(json.dumps(dict(archive=str(a),bytes=a.stat().st_size,sha256=hashlib.sha256(a.read_bytes()).hexdigest(),files=len(set(files)))))
