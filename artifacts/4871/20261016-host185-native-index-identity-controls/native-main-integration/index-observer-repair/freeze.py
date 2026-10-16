from pathlib import Path
import os,json,hashlib,tarfile,difflib
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'native-integrated';B=D/'native-main-integration/index-observer-repair'
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def dump(p,x):p.write_text(json.dumps(x,indent=2,sort_keys=True)+'\n')
selection=D/'native-decision/root-index-observer-repair-selection.md';assert sha(selection)=='7c9941e990ba64cfd413ec66ffd2bf649d6fe829c599f20052d33d940f07fdfe'
basepath=D/'native-main-integration/fixture-inventory-final/source-final.json';old=json.loads(basepath.read_text());assert sha(basepath)=='041b0ddfca3581c2b773ceb857a27b2dd4c718775b45a9cb71d0c7cc3107744d'
source={}
for base,ds,fs in os.walk(W):
 ds[:]=[d for d in ds if d!='.git']
 for f in fs:
  p=Path(base,f);q=str(p.relative_to(W))
  if q!='.git' and p.is_file():source[q]=sha(p)
source=dict(sorted(source.items()));changed=[q for q in source if source[q]!=old.get(q)]
allowed={'TreeDB/db/native_mvcc_seal_preparation_v6.go','TreeDB/internal/rootpublication/primary_empty_candidate_work_v6.go','TreeDB/pager/write_work.go','TreeDB/mvcc/native_prune_backend_route_v6_test.go','TreeDB/db/native_index_observer_v6_test.go','TreeDB/internal/rootpublication/native_index_observer_v6_test.go'}
assert set(changed)==allowed and set(old)<=set(source) and len(source)==10377
priorarchive=D/'native-main-integration/fixture-inventory-final/source-final.tar.gz';assert sha(priorarchive)=='963f9deb573465a5e56a9e2d94841058851bbc2ae28e01cc7533e7cee7106182'
with tarfile.open(priorarchive) as tf:prior={m.name:tf.extractfile(m).read() for m in tf if m.isfile() and m.name in changed}
assert all(hashlib.sha256(prior[q]).hexdigest()==old[q] for q in changed if q in old)
dump(B/'source-frozen.json',source)
if not (B/'source-frozen.tar.gz').exists():
 with tarfile.open(B/'source-frozen.tar.gz','w:gz') as tf:
  for q in source:tf.add(W/q,arcname=q,recursive=False)
with tarfile.open(B/'source-frozen.tar.gz') as tf:assert {m.name:hashlib.sha256(tf.extractfile(m).read()).hexdigest() for m in tf if m.isfile()}==source
rows=[]
with (B/'index-observer-repair.delta.patch').open('w') as out:
 for q in changed:
  before=prior.get(q,b'');after=(W/q).read_bytes()
  out.writelines(difflib.unified_diff(before.decode().splitlines(True),after.decode().splitlines(True),fromfile='a/'+q if q in old else '/dev/null',tofile='b/'+q,n=4))
  dest=B/'delta-files'/q;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(after)
  rows.append({'path':q,'before_sha256':old.get(q),'after_sha256':source[q],'status':'modified' if q in old else 'added','bytes':len(after)})
dump(B/'changed-files.json',rows)
# Exact nonobserver production bodies remain unchanged in the three affected production files.
proof={}
for q,marker in [('TreeDB/db/native_mvcc_seal_preparation_v6.go','// Containing storage belongs to the SAME admitted selected-delete producer.'),('TreeDB/pager/write_work.go','func pagerReserve(')]:
 a=prior[q].decode();b=(W/q).read_text()
 if 'pager/' in q:a=a[:a.index('// Exact retained-file binding, without Stat, duplicate, reopen or descriptor')]
 aa=a[a.index(marker):].strip();bb=b[b.index(marker):].strip();assert aa==bb
 proof[q]={'production_suffix_sha256':hashlib.sha256(bb.encode()).hexdigest(),'unchanged':True}
q='TreeDB/internal/rootpublication/primary_empty_candidate_work_v6.go';a=prior[q].decode();b=(W/q).read_text();mark='// Compare the duplicate';oldmark='func (token *StableResourceToken) MaintenanceNativeCaptureBindingForTestV6'
a=a[:a.index('// Exact comparisons stay within the original token/callback/file operands.')];b=b[:b.index(mark)];a=a.replace('\n\t"github.com/snissn/gomap/TreeDB/pager"','');assert a.rstrip()==b.rstrip();proof[q]={'prefix_except_removed_unused_import_unchanged':True}
dump(B/'invariance.json',{'all_other_original_files_byte_exact':len(old)-4,'deletions':0,'production_body_proofs':proof,'new_tests':2,'production_layouts_F293_R145_caps_lifetime_unchanged':True})
r={'source_manifest_sha256':sha(B/'source-frozen.json'),'source_archive_sha256':sha(B/'source-frozen.tar.gz'),'source_files':len(source),'changed_files':6,'modified':4,'added':2,'deleted':0,'base_source_manifest_sha256':sha(basepath),'selection_sha256':sha(selection),'delta_sha256':sha(B/'index-observer-repair.delta.patch'),'changed_inventory_sha256':sha(B/'changed-files.json'),'invariance_sha256':sha(B/'invariance.json'),'compile_executed':False,'test_bodies_executed':False,'native_publication_executed':False,'status':'Q32 RED/ACK0/noAllowanceChosen'}
dump(B/'freeze-receipt.json',r);print(json.dumps(r,indent=2))
