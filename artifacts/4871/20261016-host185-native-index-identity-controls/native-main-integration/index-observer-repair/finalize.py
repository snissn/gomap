from pathlib import Path
import json,hashlib
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');B=D/'native-main-integration/index-observer-repair'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def dump(p,x):p.write_text(json.dumps(x,indent=2,sort_keys=True)+'\n')
old=json.loads((D/'native-main-integration/fixture-inventory-final/source-final.json').read_text());new=json.loads((B/'source-frozen.json').read_text())
paths=[q for q in old if q.startswith('TreeDB/pager/') and q.endswith('.go') and q!='TreeDB/pager/write_work.go'];assert all(old[q]==new[q] for q in paths)
rows=[]
for n in ['normal-TreeDB-pager','race-TreeDB-pager']:
 p=D/'native-main-integration/fixture-controls'/n;r=json.loads((p/'result.json').read_text());assert r['exit_code']==0 and r['source_before_after_frozen'] and r['group_and_children_empty']
 rows.append({'call':n,'command':str(p/'command.json'),'command_sha256':sha(p/'command.json'),'result':str(p/'result.json'),'result_sha256':sha(p/'result.json'),'exit_code':r['exit_code']})
dump(B/'pager-prior-control-applicability.json',{'status':'source applicability only; no rerun','named_control':'TestNativeRetainedFileFencePreservesDirtyAndRetiredIdentityV6','source_base':sha(D/'native-main-integration/fixture-inventory-final/source-final.json'),'new_source':sha(B/'source-frozen.json'),'unchanged_pager_go_files':{q:new[q] for q in paths},'write_work_production_suffix':json.loads((B/'invariance.json').read_text())['production_body_proofs']['TreeDB/pager/write_work.go'],'prior_calls':rows,'limits':'Observer helper removal only in write_work.go; all original retained-file fence production and test bodies unchanged. Not new-source execution, native publication or real Finish qualification.'})
dump(B/'metadata-disposition.json',{'raw_command_preserved':True,'command_sha256':sha(B/'verification/compile-only/command.json'),'stale_field':'source_only_selection_sha256','stale_value':'54c32af15d0e0db95fabdd7a28f4d1e41280b32fe3eb6faca327e74d426c760c','cause':'copied compile proposal template retained an origin-repair source-only metadata field','actual_governing_field':'grant_sha256','actual_selection_sha256':'7c9941e990ba64cfd413ec66ffd2bf649d6fe829c599f20052d33d940f07fdfe','verification':'runner asserts actual selection hash before any launch, identity records same grant; exact argv/source/archive/actual process receipts bind this compile','disposition':'metadata defect only; no rerun or source change; stale field does not authorize or rebind execution'})
p=B/'HANDOFF.md';s=p.read_text();s+='''
## Completed one-shot compile and lease release

Compile-only PASS: all four selected packages report no tests to run, 0 test RUN, exit0, 68.080657s. Pinned Go1.26.3 and compiler hashes match the grant. Actual PID/PGID1010493 joined; subreaper verified, no residual group/direct/adopted children, timeout or signals. All10,377 before/after hashes match frozen9d6c8de3. Complete raw/command/source/process/compiler receipts are under verification/. Source and runtime leases are RELEASED; only packet artifacts completed after the join.

metadata-disposition.json preserves and explains stale source_only_selection_sha256=54c32af1 inherited in the proposal/command template. Actual grant_sha256=7c9941, selection assertion, source/archive and launch receipts are correct. Original command raw remains unchanged; no rerun.

pager-prior-control-applicability.json verifies all pager Go files except the removed invalid observer are byte-identical, and the remaining write_work.go production suffix is exact. Prior normal/race retained-file controls both PASS with original source and process joins. This is source applicability, not a new execution or native publication claim; the four new proposed calls deliberately avoid replaying that unchanged primitive.
''';p.write_text(s)
p=B/'proposed-focused-controls.json';v=json.loads(p.read_text())
for c in v['commands']:
 if 'db-original' in c['name']:
  c['required_child_tests']=['TestNativeIndexObserverOriginalCapturesV6/'+x for x in ['DATA','PRIMARY']]+['TestNativeStablePagerPartialTokenCancellationV6/'+x for x in ['before-token','growth-refusal','token-only','before-pinned-backing','before-retain','after-retain']]+['TestNativePagerOperationsPartialConstructionAndCancellationV6/'+x for x in ['before-output','output-only','custody-before-owner','complete','resume-output','resume-custody','owner-overflow']]
 else:c['required_child_tests']=['TestNativeIndexObserverOriginalBackingV6/'+x for x in ['platform','volume','object','generation','missing-backing','wrong-file','wrong-refs','zero-refs','wrong-owner','wrong-registry','missing-pin','wrong-pin-identity','released']]
dump(p,v)
manifest={str(p.relative_to(B)):sha(p) for p in sorted(B.rglob('*')) if p.is_file() and p.name not in ['packet-manifest.json','final-receipt.json'] and '/tmp/' not in str(p)}
dump(B/'packet-manifest.json',manifest)
dump(B/'final-receipt.json',{'source_manifest_sha256':sha(B/'source-frozen.json'),'source_archive_sha256':sha(B/'source-frozen.tar.gz'),'handoff_sha256':sha(B/'HANDOFF.md'),'packet_manifest_sha256':sha(B/'packet-manifest.json'),'compile_summary_sha256':sha(B/'verification/summary.json'),'compile':'PASS4packages/0testbodies','source_and_runtime_leases':'RELEASED','test_bodies_executed':False,'actual33_executed':False,'native_publication_executed':False,'status':'Q32 RED/ACK0/noAllowanceChosen'})
print((B/'final-receipt.json').read_text())
