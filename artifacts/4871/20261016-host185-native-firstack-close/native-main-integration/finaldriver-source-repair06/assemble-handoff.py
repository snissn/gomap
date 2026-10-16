from pathlib import Path
import json,hashlib,copy
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');O=D/'native-main-integration/finaldriver-source-repair06';P=D/'native-main-integration/finaldriver-source-repair05'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def put(n,x):(O/n).write_text(json.dumps(x,sort_keys=True,indent=2)+'\n')
f=json.loads((O/'freeze.json').read_text());m=json.loads((O/'source-frozen.json').read_text());old=json.loads((P/'source-frozen.json').read_text())
put('caller-disposition.json',{
 'scope':'only ordinary synchronous Coordinator.JoinNativePublicationsV6 and focused synthetic receipt-owner tests; no cursor or receipt interface change',
 'production_callers':[
 {'path':'TreeDB/internal/rootpublication/coordinator.go:828','caller':'Coordinator.Stop','disposition':'Join before scheduler stop; existing nativeDispatch barrier retains incomplete graph; settled error still reaches stopErr and scheduler teardown'},
 {'path':'TreeDB/db/db.go:3529','caller':'DB.close before exclusive teardown gate','disposition':'owner callback outside teardownMu; outstanding-native check unchanged'},
 {'path':'TreeDB/db/db.go:3551','caller':'DB.close gate recheck loop','disposition':'release gate before callback; original registration/closing barrier unchanged'}],
 'receipt_writers':[
 {'path':'TreeDB/internal/rootpublication/native_publication_v6.go:100','symbol':'BindNativePublicationV6','effect':'only production list insertion, under c.mu; unchanged'},
 {'path':'TreeDB/internal/rootpublication/native_publication_v6.go:564','symbol':'completeNativePublicationWithWorkV6','effect':'only production unlink, under c.mu after original ownership/counters/builder/pending validation; unchanged'}],
 'owner_and_terminal':{
 'path':'TreeDB/mvcc/prune_cursor_v6.go','symbols':['PruneCursor.Close','PruneCursor.CloseNativePublicationV6','PruneCursor.CloseNativePublicationStepV6','PruneCursor.closeLockedV6','PruneCursor.closeStepLockedV6'],
 'ordering':'original cursor mutex serializes physical phases; close phase0 waits for original current-delete completion/unlink then clears whole prepared value; containing lease returned only after original terminal detaches all aliases; no edit',
 'join_after_callback':'saved receipt address compared only to canonical live nodes under c.mu; absence settled regardless detached fields; found live node supplies firstError/refusedCompletion; live, owner and saved receipt aliases cleared before completion.Close',
 'first_error':'callback error retained locally through later receipt joins; live fenced error first-wins copied under c.mu; incomplete unfenced error retains existing protocol wrapping; recovery-required early path unchanged',
 'lease_refusal':'only still-linked original receipt retains returned lease; no refund on incomplete callback; duplicate lease panic unchanged; tests explicitly drain their fixture-owned error graph, not a new production retry/disposal mechanism'},
 'concurrency_boundary':{
 'source_supported':'original owner mutex prevents duplicate physical phases; after callback there are no detached receipt field reads; synthetic two-Join and Join+original-Close tests keep owner externally alive',
 'unresolved':'a concurrent public Close may refund the containing admission while another join holds a local owner alias; no new retained edge or handshake selected/implemented; Go reachability and race freedom do not prove retained accounting closure',
 'selected_actual_schedule':'one DB.Close join of paused original cursor; no concurrent public Close/multiple Join accounting claim',
 'receipt_address_rebinding':'no receipt rebinding is introduced; original terminal closes that cursor; general concurrent/new-batch accounting is not qualified'}})
put('pricing-and-lifetime.json',{
 'native_F_records_delta':0,'native_F_bytes_delta':0,'G_expression_changed':False,'G_numeric_allowance_chosen':False,'containing_size_delta':0,'retained_P_delta':0,'new_production_allocations':0,
 'reason':'new loop belongs to existing ordinary synchronous Join, with no OrdinalScanWork argument. Original finite closeStep units and their same frozen limits remain unchanged; closeLocked resets work between units under existing ordinary Close policy.',
 'new_join_work':{'under_lock':'one original live-list pointer comparison and next-pointer read per examined canonical node after each callback, plus fixed local/result checks','selected_single_owner_success':'after genuine unlink there are zero remaining live nodes; no detached receipt load','selected_still_live_refusal':'head match takes one canonical node; existing fenced/error/parked-lease checks remain on that node','general_n_successes':'with n initially live receipts and no insertions, successful head callbacks scan n-1,n-2,...,0 nodes: n*(n-1)/2 comparisons; new producers/partial callbacks follow ordinary synchronous driver policy'},
 'quota_boundary':'G covers each original finite invocation/unit, not the aggregate number of synchronous Close/Stop units or total DB.Close traffic. No fake native reservation or claimed whole-DB.Close latency bound.',
 'alias_lifetime':'same containing completion lease; callback may clear embedded receipt before return; canonical membership is authoritative. No read/write/lease parking into detached receipt. Owner and receipt locals cleared before closing returned lease.',
 'unsupported_concurrency':'synthetic caller race checks do not close concurrent public Close/local owner alias retained-admission lifetime; no new keeper/ref/owner/registry',
 'caps':'Q32/1MiB/33/80/4096 unchanged; Q32 product gate remains RED and original public ACK0; separately named diagnostic onceACK evidence remains distinct'})
keys=['TreeDB/mvcc/native_finaldriver_diagnostic_v6_test.go','TreeDB/mvcc/native_finaldriver_envelope_test.go','TreeDB/mvcc/native_finaldriver_precursor_test.go','TreeDB/mvcc/prune_cursor_v6.go','TreeDB/db/native_mvcc_final_publication_v6.go','TreeDB/db/db.go','TreeDB/internal/rootpublication/coordinator.go','TreeDB/internal/rootpublication/primary_empty_candidate_work_v6.go','TreeDB/db/native_prebuilder_pricing_for_test_v6.go']
assert all(m[k]==old[k] for k in keys)
put('invariance.json',{'exact_changed_paths':[x['path'] for x in json.loads((O/'changed-files.json').read_text())],'unchanged_semantic_inputs':{k:m[k] for k in keys},'production_layout_unchanged':True,'F_G_retained_expressions_unchanged':True,'actual_fixture_unchanged':True,'all_parent_files_except_native_publication_unchanged':True,'prior_five_actual_PASS':'historical repair05 exact source result, not a fresh repair06 result','prior_actual_rows_candidate_applicability':'ready/stale/frozen/pruner normal cleanup ends cursor before DB.Close; no source changes to their fixture/preparation/F/G/caps. Root may reuse only that narrow unchanged path proof; this packet proposes fresh onceACK and pausedClose, not blanket current-head qualification.'})
parent=json.loads((P/'proposed-real-commands-root.json').read_text());baseenv=parent['commands'][0]['env']
names=['TestNativePublicationJoinAcceptsScrubbedOriginalV6','TestNativePublicationJoinRetainsLinkedCompletionV6','TestNativePublicationJoinConcurrentScrubbedOriginalV6','TestNativePublicationStopJoinsIdleOriginalBeforeSchedulerStop','TestNativePublicationStopSettlesFailedOriginalAndStillStops','TestNativePublicationLateRefusalRequiresCustodyWithoutPoisoningSuccessor']
children={names[0]:['success','first-error'],names[1]:['unfenced','fenced-first-error'],names[2]:['two-joins','join-and-original-close']}
commands=[]
for race in (False,True):
 name=('race' if race else 'normal')+'-rootpublication-join';argv=[str(D/'toolchains/go1.26.3/bin/go'),'test','-p=1','-parallel=1','-count=1','-v','-timeout=3m']
 if race:argv.append('-race')
 argv+=['-run','^('+'|'.join(names)+')$','./TreeDB/internal/rootpublication'];env=copy.deepcopy(baseenv);env['TMPDIR']=str(O/'controls'/name/'tmp')
 commands.append({'name':name,'argv':argv,'env':env,'cwd':str(D/'native-integrated'),'executed':False,'outer_timeout_seconds':600,'required_top_level_tests':names,'required_child_tests':children,'expected_top_level_count':6,'expected_child_count':6,'behavior':'synthetic original receipt scrub/refusal/concurrency plus existing ordinary Coordinator Stop, paused original receipt and ordinary durable successor controls; no real native final driver'})
files=['TreeDB/internal/rootpublication/native_publication_join_scrub_v6_test.go','TreeDB/internal/rootpublication/native_publication_v6_test.go']
put('proposed-regression-commands.json',{'status':'PROPOSED_NOT_SELECTED_UNEXECUTED','commands':commands,'selected_test_files':{k:m[k] for k in files},'source_manifest_sha256':f['source_manifest_sha256'],'source_archive_sha256':f['source_archive_sha256'],'normal_before_race':True,'first_failure_stop':True,'concurrency_scope':'synthetic externally kept-alive owner only; not a concurrent containing-admission proof'})
real=[]
for oldc in parent['commands']:
 if oldc['name'] not in ['real-once-ack','real-paused-finish-close']:continue
 c=copy.deepcopy(oldc);c['executed']=False;c['env']['TMPDIR']=str(D/'native-main-integration/finaldriver-real-controls-repair06'/c['name']/'tmp');real.append(c)
assert len(real)==2,[x['name'] for x in real]
put('proposed-real-commands.json',{'status':'PROPOSED_NOT_SELECTED_UNEXECUTED','commands':real,'source_manifest_sha256':f['source_manifest_sha256'],'source_archive_sha256':f['source_archive_sha256'],'selected_test_files':{k:m[k] for k in parent['selected_test_files']},'G_source':parent['G_source'],'G_or_allowance_changed':False,'actual_input_source_unchanged':True,'order':'once-ack then paused-finish-close','requires_root_explicit_runtime_grant':True,'reused_rows':'no blanket reuse. Prior ready/stale/frozen/pruner PASS remains repair05 historical evidence; only root can select exact path applicability.'})
(O/'README.md').write_text('''Repair06 corrects the demonstrated original Close join after genuine Finish.

The original cursor completes and unlinks its embedded publication receipt, then
clears its entire preparation before returning the containing accounting lease.
Join previously read `r.complete` after that callback, saw the scrubbed zero, and
could park the lease into detached storage. The sole production change compares
the saved address with canonical live-list nodes under the existing c.mu. Only a
matched live node may supply error/lease fields. Absence establishes settled
completion; the owner/receipt aliases are cleared before lease.Close. Late first
error is copied before unlocking, removing the sibling unlocked field read.

The new focused source controls use actual Bind/Complete and an original-owner
fixture that immediately scrubs the embedded receipt. They cover remaining live
receipts, first-error preservation, linked refusal/parked lease/replay, and two
concurrent synthetic owner schedules. Existing Stop and late-refusal controls
are proposed alongside them. Every proposed command is unexecuted. No Go,
compiler, test body, actual publication or benchmark ran under this source lease.
Only pinned gofmt executed. The repair05 failure and source archives are intact.

Scope and accounting limits

There is no receipt/interface/layout/new-owner change, and no change to native
F, Finish unit prices, retained P or the derived diagnostic G expression. Join is
an ordinary synchronous external driver; the new canonical traversal is recorded
separately in pricing-and-lifetime.json. G never bounds total DB.Close work.
General synchronous Join may scan the remaining list after each callback; in the
selected sole-owner successful case the list is empty after unlink.

Concurrent synthetic callers prove original mutex/callback behavior only. The
test keeps its owner externally reachable. A concurrent public Close refunding
its containing admission while another Join holds an owner alias remains an
explicit retained-lifetime boundary, not repaired by Go reachability or a race
PASS. No new keeper, registry, epoch or handshake was added. The selected actual
paused-Finish diagnostic has one DB.Close owner/join and no competing public
Close. Full product/concurrency acceptance remains open.

Freeze: source-frozen.json and source-frozen.tar.gz bind all 10,433 files; two
paths differ from repair05 (one original production file, one new test file).
All 10,431 other parent paths and the raw index remain exact. See freeze.json,
changed-files.json, changed-source.tar.gz and source.delta.patch. Source/runtime
leases are released; remaining work requires root review and explicit runtime
selection. Original Q32/1MiB/caps are unchanged; Q32 remains RED/ACK0.
''')
arts=['README.md','caller-disposition.json','changed-files.json','changed-source.tar.gz','invariance.json','pricing-and-lifetime.json','proposed-real-commands.json','proposed-regression-commands.json','source.delta.patch']
put('handoff.json',{'status':'FROZEN_SOURCE_ONLY_AWAIT_ROOT_REVIEW_RUNTIME_SELECTION','freeze':f,'artifacts':{n:sha(O/n) for n in arts},'go_or_tests_executed':False,'source_lease_released':True,'runtime_lease_released':True,'regression_proposal':'two sequential normal/race calls, six top/six children per mode','actual_proposal':'fresh once-ack and paused-finish-close; root explicit grant required','remaining':'independent source review; selected focused regression then affected actual route. Concurrent containing-admission handshake not implemented/qualified.'})
print(json.dumps({'handoff_sha256':sha(O/'handoff.json'),'focused_proposal_sha256':sha(O/'proposed-regression-commands.json'),'actual_proposal_sha256':sha(O/'proposed-real-commands.json')},indent=2))
