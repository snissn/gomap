from pathlib import Path
import json,hashlib,copy,re
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015'); W=D/'native-integrated';O=D/'native-main-integration/finaldriver-source-repair07';P=D/'native-main-integration/finaldriver-source-repair06'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def put(n,x):(O/n).write_text(json.dumps(x,sort_keys=True,indent=2)+'\n')
f=json.loads((O/'freeze.json').read_text());m=json.loads((O/'source-frozen.json').read_text());old=json.loads((P/'source-frozen.json').read_text())
source_names=['TreeDB/internal/rootpublication/native_publication_v6_test.go','TreeDB/internal/rootpublication/native_publication_join_scrub_v6_test.go','TreeDB/internal/rootpublication/native_publication_v6.go','TreeDB/internal/rootpublication/coordinator_service_work_v6.go','TreeDB/internal/rootpublication/coordinator.go','TreeDB/internal/rootpublication/durable_root_transaction_test.go','TreeDB/internal/rootpublication/durable_root_transaction.go','TreeDB/internal/rootpublication/coordinator_handoff_prepare_test.go','TreeDB/internal/rootpublication/resource_set.go','TreeDB/freelist/generation_v1_txn.go','TreeDB/freelist/allocator_cow.go']
put('diagnosis.json',{
 'source_bindings':{k:m[k] for k in source_names},
 'failed_parent_raw':{'path':str(P/'controls/normal-rootpublication-join/run.log'),'sha256':sha(P/'controls/normal-rootpublication-join/run.log'),'source_manifest_sha256':f['parent_manifest_sha256'],'status':'preserved; normal stopped; race and real commands not run'},
 'actual_first_refusal':{'symbol':'Coordinator.ReleaseNativePublicationTurnV6','path':'TreeDB/internal/rootpublication/native_publication_v6.go','predicate':'len(group.members)!=1','cause':'candidateWithEmptyResourceSet constructs an empty durableRootGroup. Begin accepts it, Release rejects before result/clock/transaction processing. t.Helper attributes both errors to the calling test line; raw alone does not distinguish Begin/Release.'},
 'superseded_preliminary_causes':['FakeClock unsupported at Begin is false: admitNativeClockLockedV6 explicitly supports *FakeClock.','ReleaseNativePublicationTurnV6 does not call AdmitNativePublicationCompletionV6; the separate completion-plan realClock guard was not this helper call chain.'],
 'secondary_bound':'A genuine singleton with FakeClock and installed timer would require Begin6+Release27=33 in the shared original32 vector. Default realClock requires Begin5+Release26=31. No cap or vector reset added.',
 'repair':'Only original test helper file: default realClock; StartupSynchronous scheduler exclusion; genuine existing ordinary PreparedCOW+transaction+candidate fixture; original Enqueue activation, original Consume separately for success, actual Release with same original work. Ambiguity keeps activated transaction unconsumed and verifies original recovery handoff/release.',
 'scheduling':'startup mode stays enabled throughout. CompleteStartup requires activeDispatch0 and is intentionally not called while receipt lives. WaitThrough registers original waiter/request then dispatchPublication(true); eligibility allows activeDispatch==nativeDispatch after native pending detach. This is the original ordinary engine driven synchronously, not background-scheduler progress.',
 'source_only_limits':'No compiler or test body executed; genuine ordinary fixture Consume invokes allocator/pager callbacks outside synthetic Begin/Release accounting. These controls do not qualify the full native turn/Consume/fence or public Q32 completion.',
 'minimal_scope':'one existing test file; no production, layout, price, retained owner, error graph or actual finaldriver fixture change'})
# exact source operation ledger; sizeof denotes actual source unsafe.Sizeof, not guessed numeric layouts.
rows=[
 {'symbol':'BeginNativePublicationTurnV6 entry','records':1,'bytes':'sizeof(NativePublicationV6)+128'},
 {'symbol':'Begin admitNativeNotifyV6','records':3,'bytes':'112+3*sizeof(chan struct{})'},
 {'symbol':'Begin admitNativeClockLockedV6 realClock','records':1,'bytes':'sizeof(Clock interface)+sizeof(time.Time)'},
 {'symbol':'ReleaseNativePublicationTurnV6 entry','records':1,'bytes':'sizeof(NativePublicationV6)+128'},
 {'symbol':'Release singleton geometry','records':2,'bytes':'sizeof(c.pending slice)+sizeof(c.waiters slice)+sizeof(durableRootGroupExtension)'},
 {'symbol':'Release admitNativeNotifyV6','records':3,'bytes':'112+3*sizeof(chan struct{})'},
 {'symbol':'Release admitNativeWakeV6','records':1,'bytes':'sizeof(chan struct{})'},
 {'symbol':'Release admitNativeTimerStopLockedV6 header','records':1,'bytes':'sizeof(Timer interface)'},
 {'symbol':'Release optional installed realTimer','records':2,'bytes':'sizeof(Timer interface)+sizeof(time.Timer)'},
 {'symbol':'Release admitNativeClockLockedV6 realClock','records':1,'bytes':'sizeof(Clock interface)+sizeof(time.Time)'},
 {'symbol':'Release singleton transaction inspection','records':1,'bytes':'sizeof(DurableRootTransaction)+sizeof(DurableRootCallbackInput)'},
 {'symbol':'Release canonical completion','records':14,'bytes':'sizeof(primaryPublicationCompletionV6)+4*sizeof(Frontier)+2*sizeof(PublishAttempt)+2*sizeof(c.pending slice)+2*sizeof(c.waiters slice)+12*sizeof(uint64)'}]
assert sum(x['records'] for x in rows)==31
put('pricing-and-lifetime.json',{'rows_success_with_timer':rows,'combined_records_success_timer_present':31,'combined_records_success_timer_absent':29,'ambiguity_combined_records_timer_present':21,'ambiguity_error_row':{'records':6,'bytes':'2*sizeof(error)+sizeof([]error)+2*sizeof(error)+sizeof(c.waiters)+sizeof(c.poison)+sizeof(c.publishNow)+sizeof(c.wakeReason)+2*sizeof(PublishAttempt)+16','replaces_success_tail':'realClock1+transaction1+canonical14'},'original_vector':{'RecordLimit':32,'ByteLimit':1048576,'same_vector_between_Begin_and_Release':True,'no_reset_or_limit_change':True},'byte_evidence':'Exact typed expressions above remain original Reserve calls; no new scalar byte allowance or numeric sizeof claim. The unchanged1MiB vector enforces bytes at execution. Compile/test remains unexecuted here. Channel size112 is the original pinnedGo1.26.3/linux/amd64 diagnostic service contract, not new retained ownership.','ordinary_fixture_work':'Existing original ordinary Prepare/Enqueue/Consume/recovery callbacks and empty Set release are explicitly outside this synthetic finite Begin/Release vector. They are not a native nil-work fallback or proof of full native atomic bound.','native_F_delta':0,'native_G_expression_delta':0,'native_P_delta':0,'production_containing_size_delta':0,'new_production_owner_or_allocation':False,'first_cause':'grouplen0, not clock rejection','lifetime':'Successful fixture transaction consumes once and releases its original owner; original Set remains Coordinator until receipt owner cleanup. Ambiguous fixture has no Consume, remains Coordinator until original TakeRecoveryHandoff moves transaction+Set together, then releases exactly those original edges. Existing failed Set controls retain original disposal assertions.','scope_boundary':'Q32 product gate RED/ACK0 remains. Synthetic control success, if observed later, does not qualify full native publication or concurrent containing-admission lifetime. Existing repair06 concurrency boundary unchanged.'})
names=['TestNativePublicationJoinAcceptsScrubbedOriginalV6','TestNativePublicationJoinRetainsLinkedCompletionV6','TestNativePublicationJoinConcurrentScrubbedOriginalV6','TestNativePublicationPausedFinishAllowsOrdinaryDurableSuccessor','TestNativePublicationStopJoinsIdleOriginalBeforeSchedulerStop','TestNativePublicationLateRefusalRequiresCustodyWithoutPoisoningSuccessor','TestNativePublicationAmbiguityCannotReleaseTurnAsSuccess','TestNativePublicationStopSettlesFailedOriginalAndStillStops']
children={names[0]:['success','first-error'],names[1]:['unfenced','fenced-first-error'],names[2]:['two-joins','join-and-original-close']}
assert len(names)==8 and sum(map(len,children.values()))==6
parent=json.loads((P/'proposed-regression-commands.json').read_text());commands=[]
for oldc in parent['commands']:
 c=copy.deepcopy(oldc);c['executed']=False;c['argv'][c['argv'].index('-run')+1]='^('+'|'.join(names)+')$';c['required_top_level_tests']=names;c['required_child_tests']=children;c['expected_top_level_count']=8;c['expected_child_count']=6;c['env']['TMPDIR']=str(O/'controls'/c['name']/'tmp');c['behavior']='all shared helper users; actual ordinary PreparedCOW/transaction Enqueue+Consume, finite synthetic Begin/Release, original ambiguity recovery and physical receipt joins; startup WaitThrough ordinary successor; no actual mvcc native finaldriver';commands.append(c)
files=['TreeDB/internal/rootpublication/native_publication_v6_test.go','TreeDB/internal/rootpublication/native_publication_join_scrub_v6_test.go','TreeDB/internal/rootpublication/durable_root_transaction_test.go']
proposal={'status':'PROPOSED_UNEXECUTED_REQUIRES_ROOT_GRANT','commands':commands,'selected_test_files':{k:m[k] for k in files},'source_manifest_sha256':f['source_manifest_sha256'],'source_archive_sha256':f['source_archive_sha256'],'normal_before_race':True,'first_failure_stop':True,'expected_total_tops':16,'expected_total_children':12,'scope':'synthetic coordinator-only accounting and original ordinary fixture callbacks; not a full native publication/Consume/G claim'}
put('proposed-regression-commands.json',proposal);put('proposed-commands.json',proposal)
real=json.loads((P/'proposed-real-commands.json').read_text())
real['source_manifest_sha256']=f['source_manifest_sha256'];real['source_archive_sha256']=f['source_archive_sha256'];real['status']='PROPOSED_UNEXECUTED_REQUIRES_ROOT_GRANT'
for c in real['commands']:
 c['executed']=False;c['env']['TMPDIR']=str(D/'native-main-integration/finaldriver-real-controls-repair07'/c['name']/'tmp')
put('proposed-real-commands.json',real)
put('caller-disposition.json',{'shared_constructor':'nativePublicationControlV6 used by all8 selected tops; no other production/test file changed','shared_success_helper':'nativeControlFenceV6 used by 4 original success/Stop/late-error controls and new JoinRetainsLinked/fenced child','separate_ambiguity':'same genuine singleton Enqueue helper; no Consume before ambiguous Release; original recovery transfer/release asserted','new_join_controls':'same original helper constructor; other new join controls have no fence and retain their unchanged actual Bind/Complete/scrub/lease assertions','scheduler':'StartupSynchronous preserves original background exclusion. Original WaitThrough dispatchPublication(true) drives successor; no CompleteStartup bypass or arbitrary delay. This proves ordinary engine progress, not ordinary background scheduler service.','production_join':'repair06 production canonical live-list membership implementation unchanged','full_native_actual_route':'unchanged mvcc diagnostic fixture uses real production preparation/Consume/Finish; proposed later only onceACK and pausedFinishClose remain separate from focused synthetic controls','prior_evidence':'repair06 first focused failure preserved; no race/actual06 result exists. No prior failure converted into pass by source reasoning.'})
assert [k for k in m if m[k]!=old.get(k)]==['TreeDB/internal/rootpublication/native_publication_v6_test.go']
put('invariance.json',{'exact_changed_paths':['TreeDB/internal/rootpublication/native_publication_v6_test.go'],'other_parent_paths_unchanged':10432,'all_production_unchanged':True,'new_join_test_unchanged':True,'actual_mvcc_test_unchanged':True,'raw_index_unchanged':True,'native_F_G_P_unchanged':True,'source_bindings':{k:m[k] for k in source_names if not k.endswith('native_publication_v6_test.go')},'historical_controls':'Initial source-focused results remain at their exact recorded source; only unchanged-source applicability is asserted here, no fresh full-head runtime acceptance.'})
(O/'README.md').write_text('''Repair07: genuine singleton coordinator test fixture, SOURCE ONLY.

The failed repair06 helper successfully admitted Begin with FakeClock but its
empty-Set-only candidate had no DurableRoot transaction. Release correctly
rejected group.members length0 before its clock/success processing. Preliminary
claims that Begin rejected FakeClock, or Release called the separate realClock-
only AdmitNativePublicationCompletionV6, were incorrect on this exact source.
The original failed raw packet remains immutable; diagnosis.json binds it.

This one-file test repair uses the existing genuine ordinary PreparedCOW and
DurableRootTransaction fixture (no payload), Enqueues it normally, checks raw
transaction phase/owner and original Set owner, and executes its original ordinary
Consume callback before successful Release. The ambiguous arm does not Consume;
it verifies original transaction+Set recovery transfer and terminal release.
No phase fields, group membership, fence fields or authority are fabricated.
All previous physical-refusal, Stop, successor, first-error and receipt assertions
remain; explicit transaction callback/custody assertions supplement them.

Default realClock replaces FakeClock. StartupSynchronous keeps the background
scheduler from stealing the prepared turn without a delay/sleep. It remains
true while native physical completion is paused: CompleteStartup correctly
requires activeDispatch0 and is not called. Original WaitThrough dispatches the
ordinary successor through the maintained synchronous startup engine. This is
ordinary-engine progress, not a new background-scheduler qualification.

Original shared Begin/Release work is still32 records/1MiB. Begin5 + success
Release24 or26 =29 or31 (no timer or installed timer). FakeClock would add2 total
and could require33. The exact per-source typed byte expressions and error branch
are in pricing-and-lifetime.json. Existing ordinary Prepare/Enqueue/Consume and
recovery callbacks run outside that synthetic vector, as explicitly identified;
this is not proof that a full native transaction/fence/Consume fitsQ32. Public
Q32 remains RED/ACK0; F/G/P and all production/layout/lifetime bytes are unchanged.

Freeze binds all10,433 source files; only native_publication_v6_test.go differs
from repair06. Full and changed archives are reconstructable; delta and both
source manifests bind exact parent/current bytes. Raw index4c6b6335 unchanged.
Only pinned gofmt ran; no compiler, test body or runtime was executed here.
Source and runtime leases are RELEASED. Root review and explicit runtime grant
are required for the proposed two focused calls (8tops/6children each) and the
separate later actual onceACK/pausedFinishClose calls. All proposal executed
flags remain false. No Git/index/GH action occurred.

The repair06 concurrent containing-admission boundary remains explicit: synthetic
mutex/callback tests with externally kept-alive owners do not prove admission
lifetime under concurrent public Close. No owner/registry/keeper was added.
''')
artifacts=['source-frozen.json','source-frozen.tar.gz','changed-source.tar.gz','source.delta.patch','changed-files.json','freeze.json','diagnosis.json','pricing-and-lifetime.json','caller-disposition.json','invariance.json','proposed-commands.json','proposed-regression-commands.json','proposed-real-commands.json','README.md']
put('handoff.json',{'status':'FROZEN_SOURCE_ONLY_REPAIR07_TEST_FIXTURE','source_lease_released':True,'runtime_lease_released':True,'source_root':str(W),'packet':str(O),'source_manifest_sha256':f['source_manifest_sha256'],'source_archive_sha256':f['source_archive_sha256'],'files':len(m),'changed_paths':1,'production_changed':False,'go_or_runtime_executed':False,'index_sha256':f['index_sha256'],'artifacts':{k:sha(O/k) for k in artifacts},'proposals_unexecuted':True,'next_action':'root immutable review then explicit focused normal/race grant; no automatic runtime'})
print(json.dumps({'handoff_sha256':sha(O/'handoff.json'),'regression_proposal_sha256':sha(O/'proposed-regression-commands.json'),'real_proposal_sha256':sha(O/'proposed-real-commands.json')},indent=2))
