from pathlib import Path
import json,hashlib,copy
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'native-integrated';P=D/'native-main-integration/finaldriver-source-repair07';O=D/'native-main-integration/finaldriver-source-repair08'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def put(n,v):(O/n).write_text(json.dumps(v,sort_keys=True,indent=2)+'\n')
f=json.loads((O/'freeze.json').read_text());m=json.loads((O/'source-frozen.json').read_text());old=json.loads((P/'source-frozen.json').read_text())
assert [k for k in m if m[k]!=old.get(k)]==['TreeDB/internal/rootpublication/native_publication_v6_test.go']
bindings=['TreeDB/internal/rootpublication/native_publication_v6_test.go','TreeDB/internal/rootpublication/native_publication_join_scrub_v6_test.go','TreeDB/internal/rootpublication/coordinator.go','TreeDB/internal/rootpublication/durable_root_transaction.go','TreeDB/internal/rootpublication/durable_root_transaction_test.go','TreeDB/internal/rootpublication/native_publication_v6.go','TreeDB/freelist/allocator_cow.go','TreeDB/freelist/generation_v1_materialize_output_work.go','TreeDB/freelist/generation_v1_reservation.go','TreeDB/internal/rootpublication/primary_transaction_construction_v6.go']
raw=P/'controls/normal-rootpublication-join/run.log'
put('diagnosis.json',{'source_bindings':{k:m[k] for k in bindings},'failed_parent':{'source_manifest_sha256':f['parent_manifest_sha256'],'raw_path':str(raw),'raw_sha256':sha(raw),'root_result_receipt_sha256':'053d5aa8e1a02e68c9f57869ed22b33b00c1720c754ac2328bc6b3b018674808','observed':'normal6tops6children PASS; two ordinary successor tops FAIL on rootless candidate2 Enqueue; no race/actual ran; preserved'},'first_throwing_seam':'Coordinator.preflightDurableRootLocked: empty group + c.durableRootSequence!=0 returns ErrDurableRootLineage before transfer/activation','chain':'repair07 genuine native Release establishes lineage1/sequence1; both remaining candidate(t,2,1) calls construct no transaction, so are invalid successors. Initial07 source review missed this directly affected caller consequence.','required_invariants':['next has singleton transaction initially Candidate-owned','next.Lineage equals canonical durable lineage; sequence exactly current+1','same original first allocator/pager and capability after first Consume installs generation1','Prepare seq2 uses that current generation, fresh candidate ID; reservation BaseID and BaseDigest match exact first generation Ref','ordinary Enqueue transfers Set then transaction and invokes original Activate; WaitThrough dispatches original Publisher+consumeDurableRootGroup','successor callback writes actual prepared pages and PublishCOWCandidate installs exact candidate generation; no manual successor Consume/phase edits','original physical Set1/receipt remain Coordinator-owned and unfinished until original Close'], 'repair_scope':'one test file; helper nativeControlFence now returns its original fixture; nativeControlSuccessor continues actual allocator and lineage for both affected callers. Every original assertion preserved; added raw transaction/callback/generation/custody checks.','runtime_status':'SOURCE ONLY; no Go/compiler/tests/actual publication executed; no production/formula change'})
put('caller-disposition.json',{'shared_constructor':'nativePublicationControlV6 unchanged from07; default realClock/StartupSynchronous retained','first_fence':'nativeControlFenceV6 only adds return of actual existing fixture; previous original singleton Enqueue/Consume/Release assertions unchanged','affected_successor_callers':['TestNativePublicationPausedFinishAllowsOrdinaryDurableSuccessor','TestNativePublicationLateRefusalRequiresCustodyWithoutPoisoningSuccessor'],'successor_setup':'new helper builds only test-owned genuine DurableRootTransaction on SAME first pager/allocator. Fresh candidateID, exact parent BaseID/BaseDigest, same lineage, next sequence. Abort/Fail/Activate/Consume use original APIs with exact PreparedCOW identity checks.','successor_execution':'ordinary Enqueue invokes original Activate; maintained WaitThrough dispatchPublication(true) invokes original Publisher success then consumeDurableRootGroup before final Set release/frontier ACK. Helper never manually consumes successor.','successor_oracles':'before WaitThrough: Activated/Coordinator, Activate1, Consume0, Set Coordinator, allocator still base; after WaitThrough: Consumed/Released, Consume1/Abort0/Fail0, Set Released, exact current allocator pointer+Ref, same canonical lineage/sequence2. First Set Coordinator, receipt fenced/uncompleted, Close0, first Consume remains1.','other_helper_users':'remaining6 tops and6children retain original07 behavior; all8tops6children proposed to cover common helper return and constructor scope','ordinary_test_model_boundary':'original Activate callback validates PreparedCOW but does not separately activate allocator vector, matching existing DurableRootTransaction fixture. Actual original Consume writes pages then PublishCOWCandidate; no new native production meaning claimed.','actual_mvcc_route':'unchanged; genuine native finaldriver/onceACK/Finish still requires root separate runtime grant; no evidence upgraded by this source repair'})
pricing=copy.deepcopy(json.loads((P/'pricing-and-lifetime.json').read_text()));pricing['repair08_delta']='No production/native F/G/P/layout change. Only test-owned ordinary successor setup now uses genuine same-allocator transaction instead of invalid rootless candidate. Its Prepare/Enqueue/WaitThrough/pager writes are original ordinary work outside the unchanged synthetic first Begin/Release vector.';pricing['runtime_claim']='No08 tests ran; the07 first singleton/control PASS is historical, not proof08 overall PASS.'
put('pricing-and-lifetime.json',pricing)
put('invariance.json',{'exact_changed_paths':['TreeDB/internal/rootpublication/native_publication_v6_test.go'],'other_parent_paths_unchanged':10432,'all_production_unchanged':True,'new_join_test_unchanged':True,'actual_mvcc_test_unchanged':True,'source_bindings':{k:m[k] for k in bindings[1:]},'raw_index_sha256':f['index_sha256'],'F_G_P_layout_unchanged':True,'ordinary_ownership_policy_unchanged':True,'old07_failure_preserved':True})
proposal=copy.deepcopy(json.loads((P/'proposed-regression-commands.json').read_text()));proposal['source_manifest_sha256']=f['source_manifest_sha256'];proposal['source_archive_sha256']=f['source_archive_sha256'];proposal['selected_test_files']={k:m[k] for k in proposal['selected_test_files']};proposal['status']='PROPOSED_UNEXECUTED_REQUIRES_ROOT_GRANT'
for c in proposal['commands']:
 c['executed']=False;c['env']['TMPDIR']=str(O/'controls'/c['name']/'tmp');c['behavior']='all8 original shared helper users and6children; genuine same-allocator ordinary successor Prepare/Enqueue/Publish/Consume, first singleton finite-control Release, original receipt scrub/refusal/Stop/recovery; no actual mvcc native finaldriver'
put('proposed-regression-commands.json',proposal);put('proposed-commands.json',proposal)
real=copy.deepcopy(json.loads((P/'proposed-real-commands.json').read_text()));real['source_manifest_sha256']=f['source_manifest_sha256'];real['source_archive_sha256']=f['source_archive_sha256'];real['selected_test_files']={k:m[k] for k in real['selected_test_files']};real['status']='PROPOSED_UNEXECUTED_REQUIRES_ROOT_GRANT'
for c in real['commands']:
 c['executed']=False;c['env']['TMPDIR']=str(D/'native-main-integration/finaldriver-real-controls-repair08'/c['name']/'tmp')
put('proposed-real-commands.json',real)
(O/'README.md').write_text('''Repair08: continue the genuine ordinary successor lineage and allocator.

Repair07 normal controls passed six top-level tests and six child cases, then
two successor tests failed at ordinary Enqueue of rootless candidate2. The first
genuine transaction now establishes canonical durable lineage1/sequence1, so
preflightDurableRootLocked correctly rejects a missing next transaction. That
caller consequence was missed by07 source review. Failed07 raw/result/source
remain immutable; no race or actual07 result exists.

Only native_publication_v6_test.go changes. nativeControlFence returns its SAME
original first fixture. Both successor callers use a new helper preparing fresh
candidate2 on that first pager/allocator/capability after first Consume installs
generation1. The reservation BaseID/BaseDigest match the exact parent Ref.
New transaction2 has identical lineage and consecutive sequence. Its genuine
ordinary callbacks validate exact PreparedCOW identity; Consume writes prepared
pages and calls original PublishCOWCandidate. Original Enqueue activates and
WaitThrough's maintained Publisher/Consume engine consumes it; no manual
successor Consume, fabricated phase/group, or disconnected allocator is used.

Added oracles prove raw transaction owner/phase, once-only Activate/Consume,
no Abort/Fail, successor Set release, exact installed generation pointer+Ref and
canonical lineage/sequence2. They also preserve first receipt fenced/uncompleted,
first Set Coordinator, physical Close0 and first Consume1 throughout successor
completion. All original late-error, Stop, frontier and replay assertions remain.

The existing startup dispatcher arrangement and first shared32record/1MiB
Begin/Release vector are unchanged. Ordinary test fixture Prepare/Enqueue/
Consume/WaitThrough operations remain outside that synthetic vector. No full
native-turn/Q32 claim follows. All production, nativeF/G/P, layouts, limits and
actual mvcc diagnostic source are byte-identical to07. PublicQ32 remains RED.
The concurrent containing-admission boundary documented in06/07 is unchanged.

The full10,433-file manifest/archive and exact one-file delta/changed archive
are verified against07; all other10,432 paths and raw index are unchanged.
Only pinned gofmt ran; no Go/compiler/test/runtime/Git/index/GH action occurred.
Both source/runtime leases are RELEASED. Proposed focused normal/race calls
cover all8tops/6children per call, with fresh08 TMPDIRs and executed:false.
Separate actual onceACK/pausedFinishClose proposals remain unexecuted and need
root authorization after review. No prior result is silently upgraded or retried.
''')
artifacts=['source-frozen.json','source-frozen.tar.gz','changed-source.tar.gz','source.delta.patch','changed-files.json','freeze.json','diagnosis.json','caller-disposition.json','pricing-and-lifetime.json','invariance.json','proposed-commands.json','proposed-regression-commands.json','proposed-real-commands.json','README.md']
put('handoff.json',{'status':'FROZEN_SOURCE_ONLY_REPAIR08_TEST_FIXTURE','source_lease_released':True,'runtime_lease_released':True,'packet':str(O),'source_root':str(W),'source_manifest_sha256':f['source_manifest_sha256'],'source_archive_sha256':f['source_archive_sha256'],'files':len(m),'changed_paths':1,'production_changed':False,'Go_compiler_tests_executed':False,'index_sha256':f['index_sha256'],'artifacts':{k:sha(O/k) for k in artifacts},'proposals_unexecuted':True,'next_action':'root immutable review then explicit normal/race selection; no automatic runtime'})
print(json.dumps({'handoff_sha256':sha(O/'handoff.json'),'focused_proposal_sha256':sha(O/'proposed-regression-commands.json'),'real_proposal_sha256':sha(O/'proposed-real-commands.json')},indent=2))
