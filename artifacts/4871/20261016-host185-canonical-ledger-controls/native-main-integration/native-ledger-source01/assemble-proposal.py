from pathlib import Path
import json,re,hashlib,copy
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'native-integrated';O=D/'native-main-integration/native-ledger-source01'
m=json.loads((O/'source-frozen.json').read_text());f=json.loads((O/'freeze.json').read_text());base=json.loads((D/'native-main-integration/native-minimum-scan-source01/proposed-commands.json').read_text())
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
def alltests(p):return re.findall(r'^func (Test\w+)\(t \*testing.T\)',(W/p).read_text(),re.M)
groups={
'freelist-canonical':['TreeDB/freelist/reservation_canonical_storage_v6_test.go','TreeDB/freelist/allocator_preparation_work_v6_test.go','TreeDB/freelist/allocator_publication_work_v6_test.go','TreeDB/freelist/private_control_capacity_v6_test.go'],
'freelist-consumers':['TreeDB/freelist/allocator_cow_test.go','TreeDB/freelist/generation_v1_materialize_work_test.go','TreeDB/freelist/generation_v1_metadata_cursor_test.go','TreeDB/freelist/generation_v1_metadata_reuse_test.go','TreeDB/freelist/generation_v1_reservation_work_test.go'],
'db-construction':['TreeDB/db/primary_construction_ledger_v6_test.go','TreeDB/db/primary_capsule_v6_test.go','TreeDB/db/primary_runtime_cleanup_test.go','TreeDB/db/primary_runtime_disposal_work_v6_test.go','TreeDB/db/primary_runtime_metadata_test.go'],
'mvcc-prices':['TreeDB/mvcc/native_selected_ledger_test.go','TreeDB/mvcc/native_finaldriver_envelope_test.go']}
tops={k:[t for p in v for t in alltests(p)] for k,v in groups.items()}
extra={
'TreeDB/freelist/generation_v1_test.go':['TestFreelistTxn_CandidateReservationsSurviveSupersedeAndFailure','TestFreelistTxn_AllocateSkipsAnotherCandidateReservation','TestFreelistTxn_AppendFallbackSkipsAnotherCandidatesMetadataTail','TestFreelistTxn_ReservationRejectsAppendChosenBeforeCompetingMetadataTail','TestFreelistTxn_PartialSinkFailureBurnsTailUntilRetryPublishesAbandonment','TestReservationLedger_VisibleAndAmbiguousStatesRetainOwnership','TestFreelistGenerationV1_ReopenReconstructsReservationOwnership'],
'TreeDB/db/primary_offline_pair_v6_test.go':['TestPrimaryOfflinePairV6ConstructionUsesRetainedParent','TestPrimaryOfflinePairV6ConstructionFailureCleansOnlyOwnedStage','TestPrimaryOfflineScopeV6PhysicalFailureRetainsControls','TestPrimaryOfflineScopeV6ParentFailureRetainsExactCustody','TestPrimaryOfflinePairV6PreservesSlotsProofsAndPointerClosure'],
'TreeDB/db/vacuum_online_legacy_test.go':['TestVacuumIndexOnlinePostSwapWriteCheckpointAndReopenUseReplacement']}
for p,ts in extra.items():
 assert all(t in alltests(p) for t in ts)
 k='freelist-consumers' if '/freelist/' in p else 'db-construction';groups[k].append(p);tops[k]+=ts
children={
'TestCanonicalLedgerClaimExactAdmissionAndNoEffectsV6':['below','exact'],
'TestCanonicalLedgerGoverningBudgetRefusalsV6':['capacity','ordinary-lease'],
'TestCanonicalLedgerConstructorCloseOriginalCustodyV6':['ordinary-prepared','failed-abort','activated'],
'TestPrivateCOWMetadataClaimLeavesSharedDATAWithOriginalTxnV6':['append','reuse'],
'TestPrivateCOWLateDATAClaimConflictHasNoPartialEffectsV6':['append','reuse'],
'TestPrivateCOWCanonicalAtomicAdmissionBoundaryV6':['one-record','Q32-atomic-refusal','below','exact'],
'TestNativeCOWPublicationDefersOnlyConservativeEagerReuseV6':['ordinary','native'],
'TestCanonicalPublicationPrefixBackingCustodyV6':['ordinary-borrow','native-refusal'],
'TestPrivateCapacitySetSemanticsAndScalingV6':['empty','one','33','257','4097'],
'TestPrivateCapacityEveryTurnRefusalAndCancelV6':['empty','one','4097']+['4097#%02d'%i for i in range(1,31)],
'TestPrivateCapacityBoundCaptureCanonicalAndStaleV6':['append','reuse'],
'TestPrivateCapacityPartialCloneAdmissionCancelV6':['empty','one','4097']+['4097#%02d'%i for i in range(1,31)],
'TestCOWPrepareProfileLimitsCoverEveryCopiedCollection':['high_water','retired_pages','allocated_pages','abandoned_append_extents','changed_chunks','replaced_metadata_pages','base_reservation_extents','base_metadata_pages','ledger_owners','ledger_candidates','ledger_burned_tail_ranges','ledger_highest_reserved_end'],
'TestAllocatorActivatedCOWGenerationsBuildAheadOfDurablePublication':['missing_first','reordered','extra'],
'TestAllocatorCOWCandidateRetirementsRollbackBeforePublish':['prepare_failure','caller_abort'],
'TestCanonicalMaterializationWholeConstructorMatchesOrdinaryV1':['tail','reuse'],
'TestMetadataChunkSearchWrapAndBound4627':['gap','one','after','limit','sentinel'],
'TestMetadataEmissionPreservesSharedDirtyBranches4627':['success','partial-write-failure'],
'TestFreelistMetadataReuseAfterPhysicalTailFailure4627':['contiguous','abandoned-earlier-reservation'],
'TestFreelistMetadataReusePlacement4627':['full_chunk','aux_prefix','aux_consumes_first_chunk','skip_fragmented_chunks','bounded_starvation'],
'TestFreelistMetadataTailPlacementAfterBurn4627':['no-burn-unchanged','live-tail-remains-owned','overflow-rejected-without-claim'],
'TestPrimaryConstructionLedgerLateHandoffCustodyV6':['success','late-drain','failed-close','runtime-release'],
'TestOriginalPrimaryRuntimeLastRootRefusalRetainsDescriptor':['fresh','recovered'],
'TestPrimaryOfflinePairV6ConstructionUsesRetainedParent':['before-offline-primary-create','before-offline-data-create'],
'TestPrimaryOfflinePairV6PreservesSlotsProofsAndPointerClosure':['vacuum','vlog'],
'TestNativeFinaldriverEnvelopeCheckedArithmeticV6':['supported-selected-shape','sum','multiply','missing-shape'],
'TestNativeSelectedLedgerObservationAdmissionV6':['zero','record-short','byte-short','exact','nil-work','nil_cursor','cursor_operands','fixed_comparisons','initial_basis','incomplete_row']}
checks=['missing-proof','unexpected-prep','opaque','no-owner','no-live','missing-backing','unavailable-row','unavailable-pair','metadata-contradiction','unequal-shared-charge','transfer-incomplete','overflow']
children['TestNativeSelectedSetLedgerCombinationsV6']=[v for b in ['kind-backing','empty-backing'] for v in [b]+[b+'/'+c for c in checks]]
commands=[]
for race in [False,True]:
 for k in ['mvcc-prices','freelist-canonical','freelist-consumers','db-construction']:
  name=('race-' if race else 'normal-')+k
  env=copy.deepcopy(base['commands'][0]['env']);env['TMPDIR']=str(O/'controls'/name/'tmp')
  pkg='./TreeDB/'+('freelist' if k.startswith('freelist') else 'db' if k.startswith('db') else 'mvcc')
  argv=[str(D/'toolchains/go1.26.3/bin/go'),'test','-p=1','-parallel=1','-count=1','-v','-timeout=3m']+(['-race'] if race else [])+['-run','^('+'|'.join(tops[k])+')$',pkg]
  cs={t:children[t] for t in tops[k] if t in children}
  commands.append(dict(name=name,argv=argv,env=env,cwd=str(W),executed=False,selection_class='focused_requires_root_runtime_grant',required_top_level_tests=tops[k],required_child_tests=cs,expected_top_level_count=len(tops[k]),expected_child_count=sum(map(len,cs.values())),outer_timeout_seconds=600,behavior={'mvcc-prices':'Pure source-derived envelope and selected constructor ledger. No real public PruneVersions.','freelist-canonical':'Actual canonical constructor/admission, monotonic pinned retirement, private prepare/claim/rollback/activation/publication and original finite cleanup; explicit unit allowance disposition a6328ee2.','freelist-consumers':'Original ordinary canonical consumers, map-equivalent semantics, Supersede, tail failure, metadata reuse, materialization and reopen.','db-construction':'Real original PRIMARY constructor/recovery/offline/vacuum and late drain/runtime cleanup; no new actual native public route.'}[k]))
for old in base['commands'][-2:]:
 c=copy.deepcopy(old);c['env']['TMPDIR']=str(O/'actual-controls'/c['name']/'tmp');commands.append(c)
files=set(p for v in groups.values() for p in v)
files.update(x['path'] for x in json.loads((O/'changed-files.json').read_text()) if x['path'].endswith('_test.go'))
files.add('TreeDB/mvcc/native_finaldriver_diagnostic_v6_test.go')
for p in files:assert sha(W/p)==m[p]
# Explicitly name every selected test and reconcile its existence to its original source.
for c in commands:
 for t in c['required_top_level_tests']:
  assert any(re.search(r'^func '+re.escape(t)+r'\(t \*testing.T\)',(W/p).read_text(),re.M) for p in files),t
out=dict(status='PROPOSED_UNEXECUTED_ROOT_RUNTIME_SELECTION_REQUIRED',selection=str(D/'native-decision/root-native-ledger-source-selection.md'),selection_sha256='63736977b62d008237db816afd58f5d7b98a3a95dc8067c0eb3fc4de37e49201',source_manifest_sha256=f['source_manifest_sha256'],source_archive_sha256=f['source_archive_sha256'],index_sha256=f['index_sha256'],commands=commands,order=[c['name'] for c in commands],focused_commands=8,actual_route_commands=2,focused_per_mode_top_level_tests=sum(len(x) for x in tops.values()),focused_per_mode_child_tests=sum(sum(len(children.get(t,[])) for t in ts) for ts in tops.values()),test_file_bindings={p:m[p] for p in sorted(files)},Go_commands_executed=0,retries=0,stop_first_failure=True,unexpected_skip_missing_RUN_drift_timeout_leak_stop=True,TMPDIR_precreate_and_write_read_unlink_check_required=True,actual_PID_PGID_subreaper_group_join_receipts_required=True,full_source_archive_index_compiler_bindings_required=True,Q32='RED/ACK0; public protected limits/oracles unchanged; no Q32 claim from canonical bounded atomic units',benchmark_commands=0)
(O/'proposed-commands.json').write_text(json.dumps(out,sort_keys=True,indent=2)+'\n')
(O/'test-inventory.json').write_text(json.dumps({'groups':groups,'top_level':tops,'children':children,'counts':{k:{'top':len(v),'children':sum(len(children.get(t,[])) for t in v)} for k,v in tops.items()},'actual_route_separate_grant':commands[-2:]},sort_keys=True,indent=2)+'\n')
print(json.dumps({'proposed_sha256':sha(O/'proposed-commands.json'),'counts':out['focused_per_mode_top_level_tests'],'children':out['focused_per_mode_child_tests'],'commands':len(commands),'bindings':len(files)},indent=2))
