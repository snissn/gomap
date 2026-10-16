from pathlib import Path
import hashlib,json,tarfile
B=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015/native-main-integration/index-observer-controls')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def dump(p,x):p.write_text(json.dumps(x,indent=2,sort_keys=True)+'\n')
s=json.loads((B/'summary.json').read_text());r=json.loads((B/'db-original-captures-normal/result.json').read_text());assert s['commands_executed']==1 and r['status']=='STOP' and r['exit_code']==1
h={'status':'FIRST_CALL_FAIL; no retry','commands_executed':1,'commands_not_executed':s['commands_not_executed'],'source_manifest_sha256':s['source_manifest_sha256'],'source_files':s['source_files'],'source_frozen':s['source_still_frozen'],'index_unchanged':json.loads((B/'index-after.json').read_text())['unchanged'],'top_RUN':len(r['top_level_RUN_names']),'child_RUN':len([n for n in r['test_RUN_names'] if '/' in n]),'top_PASS':r['top_level_pass_count'],'child_PASS':len([n for n in r['test_PASS_names'] if '/' in n]),'FAIL_names':r['test_FAIL_names'],'group_empty':r['group_and_children_empty'],'pid':r['pid'],'pgid':r['pgid'],'runtime_lease':'RELEASED','source_mutated':False,'actual33_executed':False,'native_publication_executed':False,'public_status':'Q32 RED/ACK0/noAllowanceChosen'}
dump(B/'handoff.json',h)
files=[p for p in sorted(B.rglob('*')) if p.is_file() and p.name not in ['evidence-manifest.json','evidence.tar.gz','evidence-archive-receipt.json'] and '/tmp/' not in str(p)]
manifest={str(p.relative_to(B)):sha(p) for p in files};dump(B/'evidence-manifest.json',manifest)
with tarfile.open(B/'evidence.tar.gz','w:gz') as t:
 for p in files+[B/'evidence-manifest.json']:t.add(p,arcname=str(p.relative_to(B)),recursive=False)
with tarfile.open(B/'evidence.tar.gz') as t:
 actual={m.name:hashlib.sha256(t.extractfile(m).read()).hexdigest() for m in t if m.isfile()}
assert actual==dict(manifest,**{'evidence-manifest.json':sha(B/'evidence-manifest.json')})
dump(B/'evidence-archive-receipt.json',{'archive_sha256':sha(B/'evidence.tar.gz'),'manifest_sha256':sha(B/'evidence-manifest.json'),'archive_files':len(actual),'verified':True,'source_archive_is_separate_verified_immutable_input':json.loads((B/'selection.json').read_text())['source_archive'],'source_archive_sha256':json.loads((B/'selection.json').read_text())['source_archive_sha256']})
print(json.dumps(h,indent=2));print((B/'evidence-archive-receipt.json').read_text())
