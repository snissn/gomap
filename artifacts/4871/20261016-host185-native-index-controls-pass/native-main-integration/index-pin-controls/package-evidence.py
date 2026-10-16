from pathlib import Path
import hashlib,json,tarfile
B=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015/native-main-integration/index-pin-controls')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def dump(p,x):p.write_text(json.dumps(x,indent=2,sort_keys=True)+'\n')
s=json.loads((B/'summary.json').read_text());g=json.loads((B/'selection.json').read_text())
rs=[json.loads((Path(x['result'])).read_text()) for x in s['results']]
rows=[]
for r in rs:
 rows.append({'id':r['id'],'status':r['status'],'top_RUN':len(r['top_level_RUN_names']),'child_RUN':len([n for n in r['test_RUN_names'] if '/' in n]),'top_PASS':r['top_level_pass_count'],'child_PASS':len([n for n in r['test_PASS_names'] if '/' in n]),'FAIL_names':r['test_FAIL_names'],'SKIP_names':r['test_SKIP_names'],'exit_code':r['exit_code'],'elapsed_seconds':r['elapsed_seconds'],'pid':r['pid'],'pgid':r['pgid'],'leader_joined':r['leader_joined'],'group_empty':r['group_and_children_empty'],'source_before_after_frozen':r['source_before_after_frozen'],'stop_reasons':r['stop_reasons']})
h={'status':'PASS' if s['all_authorized_pass'] else 'STOP_FIRST_FAILURE; no retry','commands_executed':s['commands_executed'],'commands_not_executed':s['commands_not_executed'],'selection_md_sha256':sha(B/'selection.md'),'selection_json_sha256':sha(B/'selection.json'),'source_manifest':g['source_manifest'],'source_manifest_sha256':s['source_manifest_sha256'],'source_archive':g['source_archive'],'source_archive_sha256':g['source_archive_sha256'],'source_tree':g['source_tree'],'source_files':s['source_files'],'source_frozen':s['source_still_frozen'],'index_unchanged':json.loads((B/'index-after.json').read_text())['unchanged'],'calls':rows,'all_groups_empty':s['all_groups_empty'],'runtime_lease':'RELEASED','source_mutated':False,'actual33_executed':False,'native_publication_executed':False,'public_status':'Q32 RED/ACK0/noAllowanceChosen','go_identity_receipt':'identity.json','command_receipts_executed_true_only_after_launch':True}
dump(B/'handoff.json',h)
lines=['# Canonical physical pin focused controls','',f"Result: {h['status']}. Executed {h['commands_executed']} of four selected calls, normal calls before race.",'','| Call | Result | Top PASS | Child PASS | PID/PGID | Joined/empty |','| --- | --- | ---: | ---: | --- | --- |']
for r in rows:lines.append(f"| {r['id']} | {r['status']} | {r['top_PASS']} | {r['child_PASS']} | {r['pid']}/{r['pgid']} | {r['leader_joined']}/{r['group_empty']} |")
lines += ['',f"Frozen source manifest `{h['source_manifest_sha256']}`, source archive `{h['source_archive_sha256']}`, root-recorded tree `{h['source_tree']}`; {h['source_files']} source files. Full source before/after and unchanged live index are retained for each launched call. Full source archive remains the separately verified immutable input, not duplicated in the evidence archive.",'','Pinned Go 1.26.3/compiler hashes, exact environment/argv, actual executed command receipts, raw logs and actual subreaper/process-group join observations are retained. Original selection templates remain executed=false; launched command receipts are executed=true. No source/index/Git/GH changes or further Go commands were performed.','',f"Commands not executed: {h['commands_not_executed']}.",'','Runtime lease released. No actual33, native final driver/publication/fence/Consume/real Finish, benchmarks, or numeric allowance. Q32/1MiB/33/80/4096 remain RED/ACK0. This scoped result does not close full retention, sustained progress, real Finish/C1/C3 or final graph qualification.','']
(B/'HANDOFF.md').write_text('\n'.join(lines))
files=[p for p in sorted(B.rglob('*')) if p.is_file() and p.name not in ['evidence-manifest.json','evidence.tar.gz','evidence-archive-receipt.json'] and '/tmp/' not in str(p)]
manifest={str(p.relative_to(B)):sha(p) for p in files};dump(B/'evidence-manifest.json',manifest)
with tarfile.open(B/'evidence.tar.gz','w:gz') as t:
 for p in files+[B/'evidence-manifest.json']:t.add(p,arcname=str(p.relative_to(B)),recursive=False)
with tarfile.open(B/'evidence.tar.gz') as t:actual={m.name:hashlib.sha256(t.extractfile(m).read()).hexdigest() for m in t if m.isfile()}
assert actual==dict(manifest,**{'evidence-manifest.json':sha(B/'evidence-manifest.json')})
dump(B/'evidence-archive-receipt.json',{'archive_sha256':sha(B/'evidence.tar.gz'),'manifest_sha256':sha(B/'evidence-manifest.json'),'archive_files':len(actual),'verified':True,'source_archive_is_separate_verified_immutable_input':g['source_archive'],'source_archive_sha256':g['source_archive_sha256']})
print(json.dumps(h,indent=2));print((B/'evidence-archive-receipt.json').read_text())
