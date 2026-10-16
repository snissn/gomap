from pathlib import Path
import hashlib,json,tarfile,difflib,os
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'native-integrated';B=D/'native-build/ordinary-integrated-source'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def dump(p,x):p.write_text(json.dumps(x,indent=2,sort_keys=True)+'\n')
rows=json.loads((D/'native-main-integration/witness-input-census.json').read_text())['rows']
with tarfile.open(B/'base-source.tar.gz') as tf:ordinary={m.name:tf.extractfile(m).read() for m in tf if m.isfile()}
with tarfile.open(D/'native-build/source-witness-repair-source/source-frozen.tar.gz') as tf:native={m.name:tf.extractfile(m).read() for m in tf if m.isfile()}
actual={}
for base,ds,fs in os.walk(W):
 ds[:]=[x for x in ds if x!='.git']
 for f in fs:
  p=Path(base,f);q=str(p.relative_to(W))
  if q!='.git' and p.is_file():actual[q]=sha(p)
assert set(actual)=={r['path'] for r in rows}
assert len(actual)==10372
census=[]
for r in rows:
 q=r['path'];h=actual[q];a=dict(r,integrated_sha256=h,ordinary_preserved=h==r['ordinary_sha256'],native_preserved=h==r['native_sha256']);census.append(a)
 if r['status'] in ['identical','ordinary-owned']:assert a['ordinary_preserved'],q
changed=[r for r in census if not r['ordinary_preserved']];adapted=[r for r in census if r['status']=='native-owned' and not r['native_preserved']]
assert len(changed)==197 and sum(r['ordinary_sha256'] is None for r in changed)==114 and len(adapted)==6
assert all(r['status'] in ['native-owned','divergent'] for r in changed)
dump(B/'source-frozen.json',dict(sorted(actual.items())));dump(B/'integration-census.json',census);dump(B/'changed-files.json',changed);dump(B/'native-owned-adaptations.json',adapted)
with tarfile.open(B/'source-frozen.tar.gz','w:gz') as tf:
 for q in sorted(actual):tf.add(W/q,arcname=q,recursive=False)
with tarfile.open(B/'source-frozen.tar.gz') as tf:assert {m.name:hashlib.sha256(tf.extractfile(m).read()).hexdigest() for m in tf if m.isfile()}==actual
for name,src,selected in [('merged-base.delta.patch',ordinary,[r['path'] for r in changed]),('witness-to-integrated.delta.patch',native,[q for q in actual if q not in native or actual[q]!=hashlib.sha256(native[q]).hexdigest()])]:
 with (B/name).open('w') as out:
  for q in selected:
   before=src.get(q,b'').decode();after=(W/q).read_text();out.writelines(difflib.unified_diff(before.splitlines(True),after.splitlines(True),fromfile=('a/'+q if q in src else '/dev/null'),tofile='b/'+q,n=4))
for r in changed:
 q=r['path'];p=B/'delta-files'/q;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes((W/q).read_bytes())
for r in census:
 if r['status'] in ['native-owned','divergent']:
  q=r['path'];p=B/'selected-action-files'/q;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes((W/q).read_bytes())
receipt={'source_manifest_sha256':sha(B/'source-frozen.json'),'source_archive_sha256':sha(B/'source-frozen.tar.gz'),'source_files':len(actual),'base_source_sha256':sha(B/'base-source.json'),'base_archive_sha256':sha(B/'base-source.tar.gz'),'integration_census_sha256':sha(B/'integration-census.json'),'merged_base_delta_sha256':sha(B/'merged-base.delta.patch'),'witness_delta_sha256':sha(B/'witness-to-integrated.delta.patch'),'selected_actions':203,'changed_vs_ordinary':len(changed),'additions':114,'modifications':83,'deletions':0,'native_owned':181,'native_owned_adaptations':len(adapted),'divergent':22,'identical_preserved':10061,'ordinary_owned_preserved':108,'source_selection_sha256':sha(D/'native-decision/root-native-ordinary-integration-selection.md'),'compile_executed':False,'tests_executed':False,'actual33_executed':False,'native_publication_executed':False,'status':'Q32 RED/ACK0'};dump(B/'receipt.json',receipt);print(json.dumps(receipt,indent=2))
