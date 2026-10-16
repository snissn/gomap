from pathlib import Path
import json,re,hashlib,datetime
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015'); B=D/'native-main-integration/origin-repair'; W=D/'native-integrated'
def dump(p,x): p.write_text(json.dumps(x,indent=2,sort_keys=True)+'\n')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
proposal=json.loads((B/'proposed-commands.json').read_text()); compile=proposal['commands'][0]
log=(B/'verification/compile-only/run.log').read_text()
reported=[]
for line in log.splitlines():
 m=re.match(r'^(ok|\?)\s+(github.com/snissn/gomap/\S+)\s+(.*)$',line)
 if m:reported.append({'status':m[1],'package':m[2],'detail':m[3]})
expected=['github.com/snissn/gomap/'+x[2:] for x in compile['argv'] if x.startswith('./')]
audit={'method':'Read retained raw log only; no rerun, source or historical summary modification','raw_log_sha256':sha(B/'verification/compile-only/run.log'),'expected_packages':expected,'reported_packages':reported,'exact_requested_order':expected==[r['package'] for r in reported],'zero_test_RUN':not re.search(r'^=== RUN',log,re.M),'no_tests_to_run_packages':sum('[no tests to run]' in r['detail'] for r in reported),'no_test_files_packages':sum('[no test files]' in r['detail'] for r in reported),'runner_summary_false_reason':'Original predicate requires16 no-tests-to-run markers; mvccadmission correctly reports no test files, so15 markers+1 no-test-files is full16-package coverage.'}
assert audit['exact_requested_order'] and audit['zero_test_RUN'] and len(reported)==16
dump(B/'verification/package-output-audit.json',audit)
groups=[('db-origins-and-source-cleanup','./TreeDB/db',[
'TestPrimaryDataCertificateV6RefusalKeepsAcceptedCache','TestPrimaryDataCertificateV6TerminalDetachesBeforeWithdrawal','TestOrdinaryPrimaryHeldRootsCrossNativeTableBoundaryV6','TestNativeRootPreparationSourceWitnessV6','TestNativeRootPreparationSourceTerminalV6','TestNativePublishedRootPreparationDrainV6']),
('arena-origins-and-queues','./TreeDB/internal/primaryarena',[
'TestOrdinaryPrimaryCountedClaimAndReleaseLeaseV6','TestNativePrimaryAccountingOnlyQueueCannotBeAssistedV6','TestPrimaryArenaOwnedReleaseQueueCannotReceiveOrdinaryAssistance','TestPrimaryCapsuleReadRootOwnsCopyAndComponentCustody','TestPrimaryCapsulePromotionCloneAdmissionAndIndependentCustody']),
('transaction-origins-and-custody','./TreeDB/internal/rootpublication',[
'TestOrdinaryPrimaryCountedLeaseConstructionPromotionAbortV6','TestNativePreparedAbortUsesOriginalBuilderOrCandidateReceiptV6','TestNativeTransactionCallbacksRefuseOpaqueV6','TestPrimaryFinishWorkRefusalResumesOriginalV6','TestNativeTransactionBindingUsesSuppliedWorkV6','TestNativeTransactionOrdinaryNilCallbacksUnchangedV6','TestPrimaryFinishResourceRefusalAndFailureCustodyV6','TestNativeTransactionCallbackErrorIdentityV6','TestNativeBuilderHandoffPreservesOriginalOnRefusalV6','TestPrimaryFinishContainingRefundJoinsOriginalCall','TestPrimaryFinishAccountingBeforeAckAndStop']),
('zipper-ordinary-failure-parents','./TreeDB/zipper',['TestPrimaryConstructionV6SiblingFailureDisposal']),
('pager-native-construction','./TreeDB/pager',['TestPrimaryReadRootConstructionRefusesBeforeAllocationV6','TestPrimaryReadRootConstructionGrowthYieldV6']),
('owner-native-strict-growth','./TreeDB/internal/retainedalloc',['TestNativeAtomicOwnerGrowthAdmitsBeforeEffects','TestNativeAtomicOwnerGrowthPricesOriginalRollback','TestNativeOwnerGrowthAdapterPreservesYield','TestNativeOwnerGrowthAdapterPreservesUnsupported'])]
commands=[]
for mode in ['normal','race']:
 for name,pkg,names in groups:
  defs='\n'.join(p.read_text() for p in (W/pkg[2:]).glob('*_test.go'))
  assert all(re.search(r'^func '+re.escape(n)+r'\(t \*testing\.T\)',defs,re.M) for n in names),name
  run='^('+'|'.join(names)+')$'; argv=[compile['argv'][0],'test','-p=1']+(['-race'] if mode=='race' else [])+[pkg,'-run',run,'-count=1','-v','-timeout=3m']
  env=dict(compile['env']);env['TMPDIR']=str(B/'future-controls'/f'{mode}-{name}'/'tmp')
  commands.append({'name':f'{mode}-{name}','cwd':str(W),'argv':argv,'env':env,'expected_top_level_RUN':names,'expected_top_level_count':len(names),'outer_timeout_seconds':600,'executed':False,'requires_root_runtime_selection':True})
future={'status':'PROPOSED ONLY; no runtime grant implied','source_manifest_sha256':sha(B/'source-frozen.json'),'commands':commands,'calls':len(commands),'top_level_normal':sum(len(x[2]) for x in groups),'top_level_race':sum(len(x[2]) for x in groups),'stop_before_remaining_calls_on':['first failure','unexpected skip','missing or unexpected named RUN','source drift','timeout','unjoined child or residual group'],'exclusions':['actual33','native final driver','native enqueue/adoption/activation','native capsule fence','native Consume/genuine Finish','benchmark/profile'],'scope':'Real ordinary SetSync/reopen and held snapshots; actual finite root/claim/queue primitives; original receipt/callback fixture controls; source witness positive receipt ordering remains source-reviewed/synthetic phase9 only.'}
dump(B/'proposed-focused-controls.json',future)
fr=json.loads((B/'freeze-receipt.json').read_text());result=json.loads((B/'verification/compile-only/result.json').read_text());identity=json.loads((B/'verification/identity.json').read_text())
assert result['exit_code']==0 and result['source_before_after_equal'] and result['group_and_children_empty'] and not result['residual_observed_before_cleanup'] and result['test_RUN_count']==0
handoff={'status':'FROZEN source; authorized compile-only PASS; focused runtime unexecuted; Q32 RED/ACK0/noAllowanceChosen','selection_sha256':fr['selection_sha256'],'source':fr,'compile':{'exit_code':result['exit_code'],'elapsed_seconds':result['elapsed_seconds'],'requested_packages':16,'test_bodies':0,'source_equal':True,'pid':result['pid'],'pgid':result['pgid'],'leader_joined':result['leader_joined'],'group_and_children_empty':result['group_and_children_empty'],'signals':result['signals'],'residual_observed_before_cleanup':result['residual_observed_before_cleanup'],'result_path':str(B/'verification/compile-only/result.json'),'package_audit_path':str(B/'verification/package-output-audit.json'),'compiler_identity':identity},'release_callsite_rows':len(json.loads((B/'release-callsite-ledger.json').read_text())),'layout_delta_bytes':0,'postactivation_F_expression':'F293 unchanged; original applicability and unobserved terms remain','Finish_expression':'R145 unchanged; no allowance or public qualification','focused_control_proposal':str(B/'proposed-focused-controls.json'),'new_test_files':3,'new_top_level_tests':4,'source_runtime_leases':'RELEASED after compile; artifact-only handoff','no_git_index_gh_changes':True}
dump(B/'handoff.json',handoff)
md=f'''# Frozen ordinary/native origin repair

This successor repairs the previous integrated source d76289, rejected after its first control. Source is frozen at `{fr['source_manifest_sha256']}`:10,375 files,24 changed paths (21 modified,3 added). Full reconstructable archive `{fr['source_archive_sha256']}` and every delta payload are retained. No source edits followed the freeze.

The selected origin repair is source-closed across constructor, claim, directory image, shared full seal, promotion and release/abort. Ordinary counted work retains original Lease/Add/Remove/rehash behavior; explicitly native work retains strict WorkLease and the SAME queue RemovalWork. Native accounting-only debt cannot transfer to ordinary/nil assistance. Original cache-refusal fixture, strict owner helpers and CI metadata remain byte-exact. See `origin-and-pricing-ledger.md`, `release-callsite-census.json` and annotated `release-callsite-ledger.json` ({handoff['release_callsite_rows']} rows).

The one authorized compile-only command passed in {result['elapsed_seconds']:.2f}s using pinned Go1.26.3, all16 requested packages, zero test RUN. PID/PGID{result['pid']} joined; Linux subreaper and group receipts show no residual process, signals or cleanup intervention. Before/after match the frozen source. Raw runner summary is preserved: its marker-only package predicate is false because15 packages report no-tests-to-run while mvccadmission reports no-test-files. `verification/package-output-audit.json` verifies the exact16-package sequence without rerunning anything.

No test body ran. Three new test files/four controls are compile-checked only. `proposed-focused-controls.json` proposes12 sequential calls ({future['top_level_normal']} normal/{future['top_level_race']} race top-level tests), exact selectors/env/deadlines and first-failure stop for root selection. It covers the original refused Lease-only cache path, real ordinary held roots across table growth/reopen, paired native no-effect refusal, accounting-only queue transfer/refund, original abort/Finish fixture parents, native source cleanup, and strict growth primitives. It is not a runtime grant.

No containing layout changes or new retained allocation/owner were introduced: retained-byte delta0. Existing phased native reservations and F293/R145 expressions are unchanged, with their prior applicability and unknown whole-invocation geometry. Ordinary protected table rehash now explicitly prices maintained geometry before effects; native unsupported rehash still refuses. Full native seal uses its actual certificate work and is separately disclosed from the Q32 phased producer. See pricing ledger for exact terms.

Source and runtime leases are released. Q32/1MiB/33/80/4096 and once-ACK guards remain unchanged; Q32 RED/ACK0, no native allowance, actual33/native publication/final-driver runtime or performance claim. No Git/index/GH mutation.
'''
(B/'HANDOFF.md').write_text(md)
files=['source-frozen.json','source-frozen.tar.gz','origin-repair.delta.patch','changed-files.json','freeze-receipt.json','origin-and-pricing-ledger.md','release-callsite-census.json','release-callsite-ledger.json','proposed-commands.json','proposed-focused-controls.json','run-verification.py','verification/identity.json','verification/summary.json','verification/compile-only/command.json','verification/compile-only/process.json','verification/compile-only/result.json','verification/compile-only/source-before.json','verification/compile-only/source-after.json','verification/compile-only/run.log','verification/package-output-audit.json','handoff.json','HANDOFF.md']
dump(B/'evidence-manifest.json',{n:{'sha256':sha(B/n),'bytes':(B/n).stat().st_size} for n in files})
print(json.dumps({'handoff_sha256':sha(B/'handoff.json'),'handoff_md_sha256':sha(B/'HANDOFF.md'),'evidence_manifest_sha256':sha(B/'evidence-manifest.json'),'proposed_calls':len(commands),'top_level_per_mode':future['top_level_normal'],'callsite_rows':handoff['release_callsite_rows']},indent=2))
