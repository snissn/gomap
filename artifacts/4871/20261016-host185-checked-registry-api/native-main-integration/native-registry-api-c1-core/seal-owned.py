from pathlib import Path
import hashlib,json,tarfile,difflib,re,stat,copy
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'native-integrated';O=D/'native-main-integration/native-registry-api-c1-core';P=D/'native-main-integration/native-registry-api-source01'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def write(n,v): (O/n).write_text(json.dumps(v,indent=2,sort_keys=True)+'\n')
before=json.loads((O/'before.json').read_text());bm=json.loads((O/'before-modes.json').read_text());parent=json.loads((P/'source-frozen.json').read_text())
assert {k:v for k,v in parent.items() if k.startswith(('TreeDB/db/','TreeDB/lifecycle/'))}==before
paths=sorted(p.relative_to(W).as_posix() for x in ['TreeDB/db','TreeDB/lifecycle'] for p in (W/x).rglob('*') if p.is_file())
after={p:sha(W/p) for p in paths};modes={p:oct(stat.S_IMODE((W/p).stat().st_mode)) for p in paths}
changed=[p for p in sorted(set(before)|set(after)) if before.get(p)!=after.get(p) or bm.get(p)!=modes.get(p)]
assert not set(before)-set(after)
write('owned-before.json',before);write('owned-after.json',after);write('owned-modes.json',modes);write('owned-before-modes.json',bm)
write('changed-files.json',{p:{'before':before.get(p),'after':after.get(p),'before_mode':bm.get(p),'after_mode':modes.get(p)} for p in changed})
with tarfile.open(O/'before.tar.gz') as ar: pre={m.name:ar.extractfile(m).read() for m in ar if m.isfile()}
assert {p:hashlib.sha256(b).hexdigest() for p,b in pre.items()}==before
patch=''
for p in changed:
 patch+=''.join(difflib.unified_diff(pre.get(p,b'').decode().splitlines(True),(W/p).read_text().splitlines(True),fromfile='a/'+p if p in before else '/dev/null',tofile='b/'+p))
(O/'source.delta.patch').write_text(patch)
for name,ps in [('owned-after.tar.gz',paths),('changed-source.tar.gz',changed)]:
 with tarfile.open(O/name,'w:gz') as ar:
  for p in ps:ar.add(W/p,arcname=p,recursive=False)
# Exact declared controls, retaining all former controls and adding changed-risk original startup/Close.
proposal=json.loads((D/'native-main-integration/native-registry-api-preparation/proposed-commands.json').read_text())
newlife=['TestReaderRegistryHeaderCompletionTimingV6','TestReaderRegistryHeaderFailureDebtV6','TestReaderRegistryHeaderCompletionJoinsInFlightV6']
newdb=['TestReaderRegistryFailedBirthRetainsGenerationV6','TestReaderRegistryLateWrapperFailureRetainsHeaderV6','TestOfflineReaderRegistryFailureRetainsOriginalScopeV6','TestReaderRegistryStartupFailureRetainsOriginalHeaderV6','TestReaderRegistryCloseRacesFailedUnregisterV6','TestReaderRegistryGenerationPhysicalDebtRetainsOriginalEdgeV6','TestOriginalStartupCleanupPhysicalRefusalRetainsLock','TestOriginalReadOnlyStartupCleanupPhysicalRefusalRetainsLock','TestOriginalStartupSuccessAndCleanRefusalConsumeOnlyOriginalPayload','TestOfflineReplacementOriginalPagerCompletion','TestOfflineSourceLateNamespaceRefusalRetainsOriginalLock','TestPrimaryConstructionLedgerLateHandoffCustodyV6']
children={
'TestReaderRegistryOwnedAdmissionV6':['header-refusal','peak-and-drain','closed-binding','outside-lock-recheck','failed-unused-refund'],
'TestReaderRegistryPositiveCompletionV6':['nil-work','short-work','different-owner'],
'TestReaderRegistryHeaderCompletionTimingV6':['unregister','unused-fast-recheck','first-hole-cleanup','partial-branch'],
'TestNativeSnapshotMinimumAcquisitionAdmissionV6':['record-remainder-short','byte-remainder-short','first-shard','last-shard'],
'TestNativeSnapshotMinimumUnsupportedGeometryV6':['whole-map','removed-fallback-summary'],
'TestNativeSnapshotMinimumEpochInvalidationV6':['initial','registry','terminal','completed-reuse','counter-before-begin-epoch'],
'TestOriginalDATAOnlySnapshotLateManagerFailureRetainedByDB':['held-snapshot','held-active-read'],
'TestPrimarySnapshotPhysicalOwnerSurvivesDBCloseAndLastRead':['ordinary','bounded'],
'TestNativeDataDependencyReleaseRechecksCurrentGenerationV6':['current','retired-after-eligibility'],
'TestOriginalReadOnlyStartupCleanupPhysicalRefusalRetainsLock':['shared-lock','caller-lock'],
'TestOfflineReplacementOriginalPagerCompletion':['consumed','physical-refusal'],
'TestOfflineSourceLateNamespaceRefusalRetainsOriginalLock':['semantic-refusal','late-parent-refusal'],
'TestPrimaryConstructionLedgerLateHandoffCustodyV6':['success','late-drain','failed-close','runtime-release']}
allbindings={}
for c in proposal['commands']:
 tops=c['required_top_level_tests']
 tops+=newlife if c['name'].endswith('lifecycle') else newdb
 assert len(tops)==len(set(tops))
 c['argv'][-1]='^('+'|'.join(tops)+')$'
 c['env']['TMPDIR']=str(O/'proposed-controls'/c['name']/'tmp')
 c['required_child_tests']={t:[t+'/'+label for label in children[t]] for t in tops if t in children}
 c['expected_top_level_count']=len(tops);c['expected_child_count']=sum(map(len,c['required_child_tests'].values()))
 c['selection_class']='focused-registry-api-c1';c['executed']=False
 for t in tops:
  matches=[]
  for p in paths:
   if p.endswith('_test.go') and re.search(r'^func '+re.escape(t)+r'\(t \*testing.T\)',(W/p).read_text(),re.M):matches.append(p)
  assert len(matches)==1,(t,matches)
  allbindings[t]={'path':matches[0],'sha256':after[matches[0]],'required_children':c['required_child_tests'].get(t,[])}
proposal['required_dynamic_children']=children
proposal['scope']='Proposal only: combined source/compiler/process grant belongs to root; no commands executed by worker.'
write('proposed-commands.json',proposal);write('test-bindings.json',allbindings)
write('parent-binding.json',{'parent_manifest':str(P/'source-frozen.json'),'parent_manifest_sha256':sha(P/'source-frozen.json'),'parent_owned_count':len(before),'owned_after_count':len(after),'changed_count':len(changed),'changed_paths':changed,'selection':str(D/'native-decision/root-native-registry-api-c1-core-source-selection.md'),'selection_sha256':sha(D/'native-decision/root-native-registry-api-c1-core-source-selection.md'),'outside_scope':'not written/frozen by this worker; root composes outside partition and retainedalloc ResetCompletedOriginal','commands_executed':False})
assert after=={p:sha(W/p) for p in paths}
assert modes=={p:oct(stat.S_IMODE((W/p).stat().st_mode)) for p in paths}
print(json.dumps({'owned':len(after),'changed':changed,'tops_per_mode':sum(c['expected_top_level_count'] for c in proposal['commands'][:2]),'children_per_mode':sum(c['expected_child_count'] for c in proposal['commands'][:2]),'manifest':sha(O/'owned-after.json'),'archive':sha(O/'owned-after.tar.gz'),'proposal':sha(O/'proposed-commands.json')},indent=2))
