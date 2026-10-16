from pathlib import Path
import datetime,hashlib,json,subprocess,tarfile
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'native-integrated';B=D/'native-main-integration/origin-repair'
def h(b):return hashlib.sha256(b).hexdigest()
def fh(p):
 s=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1048576),b''):s.update(b)
 return s.hexdigest()
assert fh(B/'source-frozen.json')=='787445cb4cbdda78d329b9c927c6e77fbe2ce48cd669214509790c3e9305b118'
assert fh(B/'source-frozen.tar.gz')=='158c914d4996fc3d4fbc9e7881518f32c1d04ccb322e708c7c8d70af562a1889'
assert fh(B/'origin-repair.delta.patch')=='56597769336950e159dae981ba010a7c45c4236f69ff30e7fc2d9993614f3065'
assert fh(B/'changed-files.json')=='98aefa1b3b6f07405d3b9c285e0059e0b8acecd684a95db45d38ea3eeea01075'
oldpath=D/'native-main-integration/inventory/source-final.json'
assert fh(oldpath)=='d76289da830b8f2c856e8f0595f37a541de99368b2e582b1ae290bdab532ce9a'
old=json.loads(oldpath.read_text());new=json.loads((B/'source-frozen.json').read_text());rows=json.loads((B/'changed-files.json').read_text())
assert len(new)==10375 and len(old)==10372 and set(old)<=set(new)
names={r['path'] for r in rows};assert len(names)==len(rows)==24
assert names=={n for n,v in new.items() if old.get(n)!=v}
assert len(set(new)-set(old))==3
actual=set(subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','-z'],cwd=W).decode().split('\0')[:-1]);assert actual==set(new)
with tarfile.open(B/'source-frozen.tar.gz') as tf:
 members=[m for m in tf if m.isfile()];assert len(members)==len(new)
 assert {m.name:h(tf.extractfile(m).read()) for m in members}==new
assert all(fh(W/n)==v for n,v in new.items())
for r in rows:
 assert r['before_sha256']==old.get(r['path'])
 assert r['after_sha256']==new[r['path']]==fh(B/'delta-files'/r['path'])
 assert r['bytes']==(W/r['path']).stat().st_size
protected=['TreeDB/db/primary_review_repairs_test.go','TreeDB/internal/retainedalloc/owner_growth_work.go','TreeDB/internal/retainedalloc/owner_work.go','.github/ci/ci_impact.json']
assert all(new[n]==old[n] for n in protected)
receipt=dict(at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),source_manifest_sha256=fh(B/'source-frozen.json'),source_archive_sha256=fh(B/'source-frozen.tar.gz'),files_verified=len(new),modified=21,added=3,deleted=0,delta_payloads_verified=len(rows),protected_unchanged={n:new[n] for n in protected},all_other_source_unchanged=True,runtime_executed=False)
p=D/'native-main-integration/origin-repair-root-verification.json';p.write_text(json.dumps(receipt,indent=2,sort_keys=True)+'\n');print(json.dumps(receipt,indent=2))
