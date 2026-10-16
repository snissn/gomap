from pathlib import Path
import json,hashlib,tarfile,datetime,copy,re
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'native-integrated';O=D/'native-main-integration/native-minimum-scan-source01';P=D/'native-main-integration/native-firstack-cost-source01'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def out(n,x):(O/n).write_text(json.dumps(x,indent=2,sort_keys=True)+'\n')
f=json.loads((O/'freeze.json').read_text());m=json.loads((O/'source-frozen.json').read_text());old=json.loads((P/'source-frozen.json').read_text());changed=json.loads((O/'changed-files.json').read_text());modes=json.loads((O/'source-modes.json').read_text())
with tarfile.open(P/'source-frozen.tar.gz') as tf: oldmodes={x.name:oct(x.mode) for x in tf}
assert all(modes[n]==oldmodes[n] for n in old)
out('mode-disposition.json',{'all_parent_modes_unchanged':True,'parent_files':len(old),'new_files':{n:modes[n] for n in m if n not in old},'changed_files':[dict(x,before_mode=oldmodes.get(x['path']),after_mode=modes[x['path']]) for x in changed]})
base=json.loads((P/'proposed-commands.json').read_text());env=base['commands'][0]['env'];go=str(D/'toolchains/go1.26.3/bin/go')
minimum=['TestNativeSnapshotMinimumAcquisitionAdmissionV6','TestNativeSnapshotMinimumTerminalScanAdmissionV6','TestNativeSnapshotMinimumHistoricalFallbackEnvelopeV6','TestNativeSnapshotMinimumUnsupportedGeometryV6','TestNativeSnapshotMinimumEpochInvalidationV6','TestNativeSnapshotMinimumChurnThenQuietProgressV6','TestNativeSnapshotMinimumHeldReaderAndPartialCaptureV6','TestNativeSnapshotMinimumConcurrentReadersV6']
ordinary=['TestMinPinnedSnapshotCommitSeqTracksCurrentAndRetiredGenerations','TestMinPinnedSnapshotCommitSeqProtectsInFlightSnapshotAcquire','TestMinPinnedSnapshotCommitSeqRescansAcquireCompletedDuringScan','TestMinPinnedSnapshotCommitSeqToleratesShortSnapshotAcquireChurn']
capture=['TestNativeMVCCSnapshotActualCaptureAndCancellation','TestNativeMVCCSnapshotPinsOwnedEmptySet','TestNativeSnapshotBirthLookupAdmissionAndOriginalCleanupV6']
children={minimum[0]:['record-remainder-short','byte-remainder-short','first-shard','last-shard'],minimum[3]:['whole-map','dirty-fallback'],minimum[4]:['initial','registry','terminal','completed-reuse','counter-before-begin-epoch']}
envelope=['TestNativeFinaldriverEnvelopeCheckedArithmeticV6'];envchildren={envelope[0]:['supported-selected-shape','sum','multiply','missing-shape']}
actual=['TestNativeSelectedOriginalSplitDiagnosticV6'];actchildren=base['commands'][0]['required_child_tests']
cmds=[]
for mode in ['normal','race']:
 for group,pkg,tops,ch,behavior in [('db-minimum','./TreeDB/db',minimum+ordinary+capture,children,'Actual saved minimum admission/interleaving/concurrency; real held-reader partial capture/cancel; ordinary retired/current minimum and original native capture cleanup. No native publication.'),('mvcc-envelope','./TreeDB/mvcc',envelope,envchildren,'Pure checked selected-envelope arithmetic, no actual cursor/publication.')]:
  name=mode+'-'+group;e=copy.deepcopy(env);e['TMPDIR']=str(O/'controls'/name/'tmp');argv=[go,'test','-p=1','-parallel=1','-count=1','-v','-timeout=3m']+(['-race'] if mode=='race' else [])+['-run','^('+'|'.join(tops)+')$',pkg]
  cmds.append({'name':name,'argv':argv,'env':e,'cwd':str(W),'executed':False,'outer_timeout_seconds':600,'required_top_level_tests':tops,'required_child_tests':ch,'expected_top_level_count':len(tops),'expected_child_count':sum(map(len,ch.values())),'behavior':behavior,'selection_class':'focused_requires_root_runtime_grant'})
for mode in ['normal','race']:
 name=mode+'-all-shared-arms';e=copy.deepcopy(env);e['TMPDIR']=str(O/'actual-controls'/name/'tmp');argv=[go,'test','-p=1','-parallel=1','-count=1','-v','-timeout=3m']+(['-race'] if mode=='race' else [])+['-run','^TestNativeSelectedOriginalSplitDiagnosticV6$','./TreeDB/mvcc']
 cmds.append({'name':name,'argv':argv,'env':e,'cwd':str(W),'executed':False,'outer_timeout_seconds':600,'required_top_level_tests':actual,'required_child_tests':actchildren,'expected_top_level_count':1,'expected_child_count':6,'behavior':'All six SAME original full-oracle real diagnostic arms, unchanged nil meter. Includes genuine native publication/Finish and requires a SEPARATE root actual-route grant; never implied by focused grant.','selection_class':'actual_route_separate_grant_required'})
files=['TreeDB/db/native_snapshot_minimum_admission_v6_test.go','TreeDB/db/native_snapshot_minimum_interleaving_v6_test.go','TreeDB/db/snapshot_view_test.go','TreeDB/db/native_mvcc_snapshot_capture_test.go','TreeDB/db/native_snapshot_birth_lookup_v6_test.go','TreeDB/mvcc/native_finaldriver_envelope_test.go','TreeDB/mvcc/native_finaldriver_diagnostic_v6_test.go']
for cmd in cmds:
 for t in cmd['required_top_level_tests']:
  assert sum(bool(re.search(r'^func '+re.escape(t)+r'\(', (W/p).read_text(),re.M)) for p in files)==1,t
selection=D/'native-decision/root-native-minimum-scan-source-selection.md'
out('proposed-commands.json',{'status':'PROPOSED_UNEXECUTED_ROOT_RUNTIME_SELECTION_REQUIRED','source_manifest_sha256':f['source_manifest_sha256'],'source_archive_sha256':f['source_archive_sha256'],'index_sha256':f['index_sha256'],'selection':str(selection),'selection_sha256':sha(selection),'commands':cmds,'order':[c['name'] for c in cmds],'focused_commands':4,'focused_per_mode_top_level_tests':16,'focused_per_mode_child_tests':15,'actual_route_commands':2,'test_file_bindings':{p:m[p] for p in files},'retries':0,'stop_first_failure':True,'unexpected_skip_missing_RUN_drift_timeout_leak_stop':True,'TMPDIR_precreate_and_write_read_unlink_check_required':True,'actual_PID_PGID_subreaper_group_join_receipts_required':True,'full_source_archive_index_compiler_bindings_required':True,'benchmark_commands':0,'Go_commands_executed':0,'Q32':'RED/ACK0; external caps unchanged; no general progress qualification'})
sourcefiles=['TreeDB/db/native_mvcc_allocator_preparation_v6.go','TreeDB/db/native_minimum_price_for_test_v6.go','TreeDB/db/db.go','TreeDB/db/native_mvcc_snapshot_v6.go','TreeDB/db/snapshot_publication_work_v6.go','TreeDB/lifecycle/registry.go','TreeDB/lifecycle/native_minimum_price_for_test_v6.go','TreeDB/mvcc/prune.go','TreeDB/mvcc/native_finaldriver_envelope_test.go']
out('source-bindings.json',{p:m[p] for p in sourcefiles if p in m})
# Invariance includes the instrumented public oracle and all benchmark/cap guards.
invariant=['TreeDB/mvcc/native_finaldriver_diagnostic_v6_test.go','TreeDB/mvcc/native_finaldriver_precursor_test.go','TreeDB/mvcc/native_selected_ledger_test.go','TreeDB/mvcc/native_firstack_cost_v6_test.go','TreeDB/mvcc/native_prune_backend_route_v6_test.go','TreeDB/db/db.go','TreeDB/db/native_mvcc_snapshot_v6.go','TreeDB/db/snapshot_publication_work_v6.go','TreeDB/lifecycle/registry.go']
assert all(m[p]==old[p] for p in invariant)
out('invariance.json',{'unchanged_bound_files':{p:m[p] for p in invariant},'all_parent_files_except_four_modifications_unchanged':10430,'benchmark_meter_and_six_arm_oracle_unchanged':True,'acquisition_producer_and_registry_protocol_unchanged':True,'public_caps_unchanged':True,'Q32_protected_fixture_unchanged':True,'Go_compiler_tests_benchmarks_executed':False})
print(json.dumps({'proposal_sha256':sha(O/'proposed-commands.json'),'source_bindings_sha256':sha(O/'source-bindings.json'),'mode_disposition_sha256':sha(O/'mode-disposition.json')},indent=2))
