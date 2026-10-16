from pathlib import Path
import json,hashlib,re
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'native-integrated';O=D/'native-main-integration/finaldriver-source'
f=json.load(open(O/'freeze.json'));manifest=json.load(open(O/'source-frozen.json'))
groups={
'mvcc':['TestNativeFinaldriverEnvelopeCheckedArithmeticV6','TestNativeCursorBatchFlushAdmissionPreservesSelectionV6','TestNativeCursorFinalLeaseCloseAdmissionV6','TestNativeCursorBatchRefundAdmissionV6'],
'primaryarena':['TestPrimaryArenaAcquireAdmitsActualLookupV6','TestPrimaryArenaCertificateAdmitsLookupBeforeTransferV6','TestPrimaryArenaAcquireDetachAdmitsLookupBeforeOwnershipChangeV6','TestPrimaryArenaPromotionSecondLookupRefusalHasNoAcquireV6','TestPrimaryArenaAcquireAfterAdmissionUsesOriginalLookupV6'],
'pager':['TestPrimaryReadRootImageLookupAdmissionV6','TestNativeGrowthOriginalOwnerAdmissionV6','TestNativeGrowthPrivateBackingCancelCustodyV6'],
'rootpublication':['TestNativeIdentityPinAtomicLastIdleRemovalV6','TestNativeIdentityUnobserveAtomicLastIdleRemovalV6','TestNativeObserveConstructorAdmissionV6','TestNativeRegistryPreCursorScanV6','TestNativeNamespaceConstructorOwnerAdmissionV6'],
'zipper':['TestMaterializedInputGrowthUsesOriginalFiniteOwnerV6'],
'db':['TestNativeSnapshotMinimumAcquisitionAdmissionV6','TestNativeSnapshotMinimumTerminalScanAdmissionV6','TestNativeSnapshotMinimumHistoricalFallbackEnvelopeV6','TestNativeSnapshotBirthLookupAdmissionAndOriginalCleanupV6','TestNativeSealInstalledClaimGrowthHandoffRefusalV6','TestPrimaryPromotionSharesImmutableLogicalRootV6','TestPrimaryPromotionCustodyRefusalHasNoPartialAcquireV6','TestNativeStablePagerPartialTokenCancellationV6','TestNativePagerOperationsPartialConstructionAndCancellationV6'],
'freelist':['TestNativeSelectedAllocatorStructuralEnvelopeV6']}
packages={k:'./TreeDB/'+('internal/' if k in ('primaryarena','rootpublication') else '')+k for k in groups}
children={
'TestNativeFinaldriverEnvelopeCheckedArithmeticV6':['supported-selected-shape','sum','multiply','missing-shape'],
'TestPrimaryArenaAcquireDetachAdmitsLookupBeforeOwnershipChangeV6':['finite','ordinary'],
'TestPrimaryReadRootImageLookupAdmissionV6':['empty','present','missing'],
'TestNativeGrowthOriginalOwnerAdmissionV6':['unbound','concrete'],
'TestNativeIdentityPinAtomicLastIdleRemovalV6':['unbound','concrete','unsupported','ordinary','nonlast','observed'],
'TestNativeIdentityUnobserveAtomicLastIdleRemovalV6':['idle','nonlast'],
'TestNativeObserveConstructorAdmissionV6':['empty','spare','collision','tombstone','doubling','bound-replacement','unsupported','capacity','ordinary'],
'TestNativeNamespaceConstructorOwnerAdmissionV6':['work-refusal','unsupported','success','duplicate-failure','ordinary'],
'TestMaterializedInputGrowthUsesOriginalFiniteOwnerV6':['constructor','entries','payload'],
'TestNativeSnapshotMinimumAcquisitionAdmissionV6':['original-Q32','byte-unit-too-large','record-remainder-short','byte-remainder-short'],
'TestNativeStablePagerPartialTokenCancellationV6':['before-token','growth-refusal','token-only','before-pinned-backing','before-retain','after-retain'],
'TestNativePagerOperationsPartialConstructionAndCancellationV6':['before-output','output-only','custody-before-owner','complete','resume-output','resume-custody','owner-overflow'],
'TestNativeSelectedAllocatorStructuralEnvelopeV6':['invalid','physical-ahead','data-overflow','inventory-overflow','chain-format']}
commands=[]
env=json.load(open(D/'native-main-integration/index-pin-controls/selection.json'))['commands'][0]['env'];env=dict(env)
for race in (False,True):
 for key,names in groups.items():
  name=('race-' if race else 'normal-')+key;sel='^('+'|'.join(names)+')$';e=dict(env);e['TMPDIR']=str(D/'native-main-integration/finaldriver-focused-controls'/name/'tmp')
  cmd=[str(D/'toolchains/go1.26.3/bin/go'),'test','-p=1','-parallel=1','-count=1','-v','-timeout=3m']+(['-race'] if race else [])+['-run',sel,packages[key]]
  commands.append({'name':name,'argv':cmd,'env':e,'cwd':str(W),'required_top_level_tests':names,'required_child_tests':{n:children[n] for n in names if n in children},'expected_top_level_count':len(names),'expected_child_count':sum(len(children.get(n,[])) for n in names),'outer_timeout_seconds':600,'race':race,'executed':False,'behavior':'pure/standalone original primitives and ordinary setup only; no native selected finaldriver/publication'})
selected={}
for cmd in commands:
 pkg=W/cmd['argv'][-1].removeprefix('./')
 for p in pkg.glob('*_test.go'):
  text=p.read_text()
  if any(re.search(r'func '+re.escape(n)+r'\(',text) for n in cmd['required_top_level_tests']):selected[p.relative_to(W).as_posix()]=manifest[p.relative_to(W).as_posix()]
x={'status':'PROPOSED_UNEXECUTED_REQUIRES_ROOT_RUNTIME_SELECTION','commands':commands,'command_count':len(commands),'normal_top_level_tests':sum(len(x) for x in groups.values()),'race_top_level_tests':sum(len(x) for x in groups.values()),'normal_child_tests':sum(map(len,children.values())),'race_child_tests':sum(map(len,children.values())),'selected_test_files':selected,'source_manifest_sha256':f['source_manifest_sha256'],'source_archive_sha256':f['source_archive_sha256'],'source_manifest':f['source_manifest'],'source_archive':f['source_archive'],'source_files':f['source_files'],'source_index_sha256':f['index_sha256'],'first_failure_stop':True,'allow_unexpected_skip':False,'actual33_allowed':False,'native_finaldriver_allowed':False,'benchmark_allowed':False,'numeric_G_grant':None,'explicit_exclusions':['TestNativeSelectedOriginalSplitDiagnosticV6','TestNativeMVCCRealBackendRouteRefusalPreservesCustodyV6','TestPrimarySlotCustodyBothSlotsOverwriteAndReopenV6'],'notes':['normal-mvcc first implicitly compiles integrated dependencies; no separate compile cycle','fresh dedicated TMPDIR must be created and write/read/unlink checked before launch','source/archive/index/compiler/env/argv and real process subreaper/group join receipts required; no retries','historical exact-phase Q32 fixture is untouched and excluded: current source truthfully refuses 256-record minimum scan']}
(O/'proposed-commands.json').write_text(json.dumps(x,indent=2)+'\n')
real=[]
for arm in ['ready-cancel','stale-before-fence','frozen-limits','once-ack','paused-finish-close']:
 name='real-'+arm;e=dict(env);e['TMPDIR']=str(D/'native-main-integration/finaldriver-real-controls'/name/'tmp')
 real.append({'name':name,'argv':[str(D/'toolchains/go1.26.3/bin/go'),'test','-p=1','-parallel=1','-count=1','-v','-timeout=3m','-run','^TestNativeSelectedOriginalSplitDiagnosticV6$/^'+arm+'$','./TreeDB/mvcc'],'env':e,'cwd':str(W),'required_top_level_tests':['TestNativeSelectedOriginalSplitDiagnosticV6'],'required_child_tests':{'TestNativeSelectedOriginalSplitDiagnosticV6':[arm]},'outer_timeout_seconds':240,'executed':False,'behavior':'genuine DB native preparation; positive arms genuinely publish and Finish; requires separate explicit grant'})
(O/'proposed-real-commands.json').write_text(json.dumps({'status':'UNEXECUTED_SEPARATE_REAL_RUNTIME_GRANT_REQUIRED','source_manifest_sha256':f['source_manifest_sha256'],'commands':real,'original_public_caps_unchanged':True,'numeric_runtime_G':None,'G_source':'nativeSelectedDiagnosticEnvelopeV6: fresh pre-cursor actual scalars, checked symbolic expression; no measured historical constant','hard_IO_failure_runtime_proposed':False,'hard_IO_reason':'no additional genuine selected injection fixture claimed; sticky production branch unchanged and requires separate verification'},indent=2)+'\n')
(O/'test-inventory.json').write_text(json.dumps({'groups':groups,'required_child_tests':children,'focused_test_files':selected,'new_real_test':'TreeDB/mvcc/native_finaldriver_diagnostic_v6_test.go','focused_top_level_per_mode':sum(map(len,groups.values())),'focused_children_per_mode':sum(map(len,children.values()))},indent=2)+'\n')
# Rebind previous immutable subreview targets without overstating changed dependencies.
reused=[]
for report in ['native-finaldriver-custody-subreview01.json','native-finaldriver-price-subreview01.json']:
 r=json.load(open(D/'native-decision'/report));matches={p:manifest.get(p)==v['sha256'] for p,v in r['targets'].items()}
 deps={p:manifest.get(p)==h for p,h in r.get('inspected_dependency_hashes',{}).items()}
 reused.append({'report':report,'report_sha256':hashlib.sha256((D/'native-decision'/report).read_bytes()).hexdigest(),'target_applicability':matches,'dependency_applicability':deps,'full_final_source_review_claimed':False})
(O/'subreview-applicability.json').write_text(json.dumps(reused,indent=2)+'\n')
print(json.dumps({'commands':len(commands),'tops_per_mode':x['normal_top_level_tests'],'children_per_mode':x['normal_child_tests'],'files':len(selected),'subreview_target_matches':[{r['report']:sum(r['target_applicability'].values())} for r in reused]},indent=2))
