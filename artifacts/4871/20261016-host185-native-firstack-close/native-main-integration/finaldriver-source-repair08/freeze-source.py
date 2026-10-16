from pathlib import Path
import os,json,hashlib,tarfile,difflib,copy,datetime
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015'); W=D/'native-integrated'; O=D/'native-main-integration/finaldriver-source-repair08'; P=D/'native-main-integration/finaldriver-source-repair07'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def out(n,v): (O/n).write_text(json.dumps(v,sort_keys=True,indent=2)+'\n')
assert os.path.ismount('/mnt/fast4tb') and os.access(O,os.W_OK)
old=json.loads((P/'source-frozen.json').read_text()); assert sha(P/'source-frozen.json')=='398fd377bd569c52341e10cc9c90d6bd0762fec7a2ffc39413b4148d0dfe1750'
assert sha(P/'source-frozen.tar.gz')=='437acc30342c067bd36ab7c4387cc9a9d57dae7d937bc6f931816c56a4a00772'
index=D/'coordinator/.git/worktrees/native-integrated/index'; index_hash=sha(index);assert index_hash=='4c6b63357522cd951fd864e661b167f52e6debcd5d5e150e002fdba478f4dbe1'
names=[]
for root,ds,fs in os.walk(W):
 ds[:]=[d for d in ds if d!='.git']
 for f in fs:
  n=(Path(root)/f).relative_to(W).as_posix()
  if n!='.git':names.append(n)
names=sorted(names);assert len(names)==10433
m={n:sha(W/n) for n in names}; changed=sorted(n for n in m if old.get(n)!=m[n]); deleted=sorted(set(old)-set(m)); assert not deleted
expected=['TreeDB/internal/rootpublication/native_publication_v6_test.go']
assert changed==expected,changed
out('source-frozen.json',m)
for filename,selected in [('source-frozen.tar.gz',names),('changed-source.tar.gz',changed)]:
 with tarfile.open(O/filename,'w:gz',compresslevel=6) as tf:
  for n in selected:tf.add(W/n,arcname=n,recursive=False)
 with tarfile.open(O/filename) as tf:
  assert sorted(tf.getnames())==selected
  for x in tf:
   assert hashlib.sha256(tf.extractfile(x).read()).hexdigest()==m[x.name],x.name
with tarfile.open(P/'source-frozen.tar.gz') as tf:
 delta=[]
 for n in changed:
  a=tf.extractfile(n).read().decode().splitlines(True) if n in old else []
  delta.extend(difflib.unified_diff(a,(W/n).read_text().splitlines(True),fromfile='a/'+n if n in old else '/dev/null',tofile='b/'+n))
(O/'source.delta.patch').write_text(''.join(delta))
out('changed-files.json',[{'path':n,'before_sha256':old.get(n),'after_sha256':m[n],'change':'modify' if n in old else 'add'} for n in changed])
assert all(sha(W/n)==h for n,h in m.items());assert sha(index)==index_hash
out('freeze.json',{'status':'FROZEN_REPAIR08_TEST_ONLY_UNEXECUTED','source_root':str(W),'source_files':len(m),'source_manifest_sha256':sha(O/'source-frozen.json'),'source_archive_sha256':sha(O/'source-frozen.tar.gz'),'parent_manifest_sha256':sha(P/'source-frozen.json'),'parent_archive_sha256':sha(P/'source-frozen.tar.gz'),'changed_paths':len(changed),'added_paths':0,'modified_paths':1,'deleted_paths':0,'unchanged_paths':len(old)-1,'changed_archive_sha256':sha(O/'changed-source.tar.gz'),'delta_sha256':sha(O/'source.delta.patch'),'index_path':str(index),'index_sha256':index_hash,'index_after_sha256':sha(index),'archive_members_verified':True,'source_after_matches':True,'go_executed':False,'runtime_executed':False,'source_lease_released':True,'runtime_lease_released':True,'gofmt_only':True,'created_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})
print(json.dumps(json.loads((O/'freeze.json').read_text()),indent=2))
