from pathlib import Path
import os,json,hashlib,tarfile,difflib,copy,datetime
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015'); W=D/'native-integrated'; O=D/'native-main-integration/finaldriver-source-repair05'; P=D/'native-main-integration/finaldriver-source-repair04'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def out(n,v): (O/n).write_text(json.dumps(v,sort_keys=True,indent=2)+'\n')
assert os.path.ismount('/mnt/fast4tb') and os.access(O,os.W_OK)
old=json.loads((P/'source-frozen.json').read_text()); assert sha(P/'source-frozen.json')=='a38583301e50103c204b47563bed43385908705bbcc1db2eb4c4105a05e099bd'
assert sha(P/'source-frozen.tar.gz')=='b0b3cf3ddcca6517d41ea33e3860437e38ba3cdbd206dda6af3831b96553f300'
index=D/'coordinator/.git/worktrees/native-integrated/index'; index_hash=sha(index);assert index_hash=='4c6b63357522cd951fd864e661b167f52e6debcd5d5e150e002fdba478f4dbe1'
names=[]
for root,ds,fs in os.walk(W):
 ds[:]=[d for d in ds if d!='.git']
 for f in fs:
  n=(Path(root)/f).relative_to(W).as_posix()
  if n!='.git':names.append(n)
names=sorted(names);assert len(names)==10432
m={n:sha(W/n) for n in names}; changed=sorted(n for n in m if old.get(n)!=m[n]); deleted=sorted(set(old)-set(m)); assert not deleted
expected=['TreeDB/db/native_prebuilder_pricing_for_test_v6.go','TreeDB/internal/rootpublication/constructed_activation_rollback_v6_test.go','TreeDB/internal/rootpublication/coordinator.go','TreeDB/internal/rootpublication/durable_root_transaction.go','TreeDB/internal/rootpublication/durable_root_transaction_native_mvcc_v6.go','TreeDB/internal/rootpublication/primary_empty_candidate_work_v6.go','TreeDB/internal/rootpublication/types.go','TreeDB/mvcc/native_finaldriver_diagnostic_v6_test.go','TreeDB/mvcc/native_prune_backend_route_v6_test.go']
assert changed==expected,changed
out('source-frozen.json',m)
for filename,selected in [('source-frozen.tar.gz',names),('changed-source.tar.gz',changed)]:
 with tarfile.open(O/filename,'w:gz',compresslevel=6) as tf:
  for n in selected:tf.add(W/n,arcname=n,recursive=False)
 with tarfile.open(O/filename) as tf:
  assert sorted(tf.getnames())==selected
  for x in tf:
   assert hashlib.sha256(tf.extractfile(x).read()).hexdigest()==m[x.name],x.name
with tarfile.open(P/'source-frozen.tar.gz') as tf:
 delta=[]
 for n in changed:
  a=tf.extractfile(n).read().decode().splitlines(True) if n in old else []
  delta.extend(difflib.unified_diff(a,(W/n).read_text().splitlines(True),fromfile='a/'+n if n in old else '/dev/null',tofile='b/'+n))
(O/'source.delta.patch').write_text(''.join(delta))
out('changed-files.json',[{'path':n,'before_sha256':old.get(n),'after_sha256':m[n],'change':'modify' if n in old else 'add'} for n in changed])
parent=json.loads((P/'proposed-real-commands-root.json').read_text())
commands=copy.deepcopy(parent['commands']);negative=copy.deepcopy(commands[0]);negative['name']='real-pruner-enabled-refusal';negative['argv'][negative['argv'].index('-run')+1]='^TestNativeSelectedOriginalSplitDiagnosticV6$/^pruner-enabled-refusal$';negative['required_child_tests']={'TestNativeSelectedOriginalSplitDiagnosticV6':['pruner-enabled-refusal']};negative['behavior']='genuine configured pruner-enabled native preparation and activation refusal; original finite cancellation and real DB.Close, no publication/ACK permitted'
commands.insert(3,negative)
for c in commands:
 c['executed']=False;c['env']['TMPDIR']=str(D/'native-main-integration/finaldriver-real-controls-repair05'/c['name']/'tmp')
selected={n:m[n] for n in parent['selected_test_files']}
real={'status':'PROPOSED_NOT_SELECTED_UNEXECUTED','commands':commands,'source_manifest_sha256':sha(O/'source-frozen.json'),'source_archive_sha256':sha(O/'source-frozen.tar.gz'),'selected_test_files':selected,'numeric_runtime_G':None,'G_source':'unchanged checked nativeSelectedDiagnosticEnvelopeV6, including updated source helper EnqueueRecords=51 and exact failure traffic; prebirth physical key/value bounds41/9','original_public_caps_unchanged':True,'hard_IO_failure_runtime_proposed':False,'ordering':'ready-cancel, stale-before-fence, frozen-limits, genuine pruner-enabled refusal, once-ack, paused-finish-close','reuse_of_old_G_or_positive_publication':False}
out('proposed-real-commands.json',real)
names_top=['TestConstructedPrimaryActivationRollbackPreservesCustodyV6','TestDurableRootActivationFailureRollsBackOwnershipBeforeVisibility','TestEnqueueBuiltValidationAndActivationFailurePreserveFinalLease','TestNativePreparedAbortUsesOriginalBuilderOrCandidateReceiptV6']
child={names_top[0]:['ordinary','native-work','native-rollback-failure'],names_top[3]:['builder','candidate']}
testfiles=['TreeDB/internal/rootpublication/constructed_activation_rollback_v6_test.go','TreeDB/internal/rootpublication/durable_root_transaction_test.go','TreeDB/internal/rootpublication/coordinator_handoff_prepare_test.go','TreeDB/internal/rootpublication/primary_prepared_abort_work_v6_test.go']
focused=[]
for race in (False,True):
 name='race-rootpublication-rollback' if race else 'normal-rootpublication-rollback';argv=[str(D/'toolchains/go1.26.3/bin/go'),'test','-p=1','-parallel=1','-count=1','-v','-timeout=3m']
 if race:argv.append('-race')
 argv+=['-run','^('+'|'.join(names_top)+')$','./TreeDB/internal/rootpublication'];env=copy.deepcopy(commands[0]['env']);env['TMPDIR']=str(O/'controls'/name/'tmp')
 focused.append({'name':name,'argv':argv,'env':env,'cwd':str(W),'executed':False,'outer_timeout_seconds':600,'required_top_level_tests':names_top,'required_child_tests':child,'expected_top_level_count':4,'expected_child_count':5,'behavior':'real original constructed PRIMARY/callback activation refusal; no successful native enqueue/adoption/publication; ordinary and work-bearing rollback, sticky injected rollback error, original finite abort plus legacy controls'})
out('proposed-regression-commands.json',{'status':'PROPOSED_NOT_SELECTED_UNEXECUTED','commands':focused,'selected_test_files':{n:m[n] for n in testfiles},'source_manifest_sha256':sha(O/'source-frozen.json'),'source_archive_sha256':sha(O/'source-frozen.tar.gz'),'normal_before_race':True,'first_failure_stop':True})
assert all(sha(W/n)==h for n,h in m.items());assert sha(index)==index_hash
out('freeze.json',{'status':'FROZEN_REPAIR05_SOURCE_ONLY_UNEXECUTED','source_root':str(W),'source_files':len(m),'source_manifest_sha256':sha(O/'source-frozen.json'),'source_archive_sha256':sha(O/'source-frozen.tar.gz'),'parent_manifest_sha256':sha(P/'source-frozen.json'),'parent_archive_sha256':sha(P/'source-frozen.tar.gz'),'changed_paths':len(changed),'added_paths':1,'modified_paths':8,'deleted_paths':0,'unchanged_paths':len(old)-8,'changed_archive_sha256':sha(O/'changed-source.tar.gz'),'delta_sha256':sha(O/'source.delta.patch'),'index_path':str(index),'index_sha256':index_hash,'index_after_sha256':sha(index),'archive_members_verified':True,'source_after_matches':True,'go_executed':False,'runtime_executed':False,'source_lease_released':True,'runtime_lease_released':True,'gofmt_only':True,'created_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})
print(json.dumps(json.loads((O/'freeze.json').read_text()),indent=2))
