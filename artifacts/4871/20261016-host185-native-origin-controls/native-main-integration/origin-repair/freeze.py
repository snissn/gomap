from pathlib import Path
import os,json,hashlib,tarfile,difflib
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'native-integrated';B=D/'native-main-integration/origin-repair'
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def dump(p,x): p.write_text(json.dumps(x,indent=2,sort_keys=True)+'\n')
selection=D/'native-decision/root-ordinary-native-origin-repair-selection.md'
assert sha(selection)=='54c32af15d0e0db95fabdd7a28f4d1e41280b32fe3eb6faca327e74d426c760c'
basepath=D/'native-main-integration/inventory/source-final.json';old=json.loads(basepath.read_text());assert sha(basepath)=='d76289da830b8f2c856e8f0595f37a541de99368b2e582b1ae290bdab532ce9a'
source={}
for base,ds,fs in os.walk(W):
 ds[:]=[d for d in ds if d!='.git']
 for f in fs:
  p=Path(base,f);q=str(p.relative_to(W))
  if q!='.git' and p.is_file(): source[q]=sha(p)
source=dict(sorted(source.items()))
assert set(old)<=set(source)
changed=[q for q in source if source[q]!=old.get(q)]
assert len(changed)==24 and len(source)==10375
protected=['TreeDB/db/primary_review_repairs_test.go','TreeDB/internal/retainedalloc/owner_growth_work.go','TreeDB/internal/retainedalloc/owner_work.go','.github/ci/ci_impact.json']
assert all(source[q]==old[q] for q in protected)
# Recover prior bytes ONLY from the already verified complete frozen source archive.
priorarchive=D/'native-build/ordinary-integrated-source/source-frozen.tar.gz'
with tarfile.open(priorarchive) as tf: prior={m.name:tf.extractfile(m).read() for m in tf if m.isfile() and m.name in changed}
# Root changed only ci metadata between compile and final; all edited inputs match.
assert all(hashlib.sha256(prior[q]).hexdigest()==old[q] for q in changed if q in old)
dump(B/'source-frozen.json',source)
with tarfile.open(B/'source-frozen.tar.gz','w:gz') as tf:
 for q in source: tf.add(W/q,arcname=q,recursive=False)
with tarfile.open(B/'source-frozen.tar.gz') as tf: assert {m.name:hashlib.sha256(tf.extractfile(m).read()).hexdigest() for m in tf if m.isfile()}==source
rows=[]
with (B/'origin-repair.delta.patch').open('w') as out:
 for q in changed:
  before=prior.get(q,b'');after=(W/q).read_bytes()
  out.writelines(difflib.unified_diff(before.decode().splitlines(True),after.decode().splitlines(True),fromfile='a/'+q if q in old else '/dev/null',tofile='b/'+q,n=4))
  dest=B/'delta-files'/q;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(after)
  rows.append({'path':q,'before_sha256':old.get(q),'after_sha256':source[q],'status':'modified' if q in old else 'added','bytes':len(after)})
dump(B/'changed-files.json',rows)
# Exhaustive static call inventory, including both wrapper definitions and callers.
ledger=[]
for q in source:
 if not q.endswith('.go'): continue
 function=''
 for n,line in enumerate((W/q).read_text().splitlines(),1):
  if line.startswith('func '): function=line
  if any(x in line for x in ['ReleaseQueueStep(', 'ReleaseNativeQueueStepV6(', 'ReleaseStep(', 'ProgressOrdinaryRelease(', 'PreserveOrdinaryReleaseQueue(', 'releaseQueueStep(']):
   ledger.append({'path':q,'line':n,'function':function,'source':line.strip()})
dump(B/'release-callsite-census.json',ledger)
receipt={'source_manifest_sha256':sha(B/'source-frozen.json'),'source_archive_sha256':sha(B/'source-frozen.tar.gz'),'source_files':len(source),'changed_files':len(changed),'modified':21,'added':3,'deleted':0,'base_source_manifest_sha256':sha(basepath),'selection_sha256':sha(selection),'delta_sha256':sha(B/'origin-repair.delta.patch'),'changed_inventory_sha256':sha(B/'changed-files.json'),'protected_unchanged':{q:source[q] for q in protected},'compile_executed':False,'test_bodies_executed':False,'native_publication_executed':False,'status':'Q32 RED/ACK0/noAllowanceChosen'}
dump(B/'freeze-receipt.json',receipt);print(json.dumps(receipt,indent=2))
