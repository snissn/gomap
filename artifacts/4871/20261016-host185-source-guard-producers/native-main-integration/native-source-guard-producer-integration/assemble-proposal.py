import json,re,hashlib,copy
from pathlib import Path
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015'); W=D/'native-integrated'; O=Path(__file__).parent; B=D/'native-main-integration/native-source-guard-core02'
old=json.loads((B/'proposed-commands.json').read_text()); children={}
for p in sorted((D/'native-main-integration').glob('*/proposed-commands.json'),key=lambda p:p.stat().st_mtime):
 try:
  for c in json.loads(p.read_text()).get('commands',[]):
   children.update(c.get('required_child_tests',{}))
 except Exception: pass
funcs={}; files={}
for p in (W/'TreeDB').rglob('*_test.go'):
 s=p.read_text(); matches=list(re.finditer(r'^func (Test\w+)\(t \*testing.T\)',s,re.M))
 for i,m in enumerate(matches):
  name=m.group(1);funcs[name]=s[m.end():matches[i+1].start() if i+1<len(matches) else len(s)];files[name]=str(p.relative_to(W))
sel={}
sel['retainedalloc']=next(c['required_top_level_tests'] for c in old['commands'] if c['name']=='race-retainedalloc')
sel['lifecycle']=next(c['required_top_level_tests'] for c in old['commands'] if c['name']=='normal-lifecycle')+['TestSourceGuardOrdinaryEdgeFiniteConsumerV6','TestSourceGuardOrdinaryPartialBirthFiniteCancellationV6','TestSourceGuardOrdinaryInstalledFiniteCancellationV6']
sel['memtable']=next(c['required_top_level_tests'] for c in old['commands'] if c['name']=='normal-memtable')
sel['primaryarena']=[n for n,p in files.items() if p.startswith('TreeDB/internal/primaryarena/')]
sel['rootpublication']=['TestReachabilityCaptureRetainsEveryConstructedRootV6','TestReachabilityCaptureRefusesActiveFinishV6','TestReachabilityEpochInvalidatesOnEnqueueAndDurablePublish','TestReachabilityEpochSurvivesRetryableFailure','TestReachabilityRevalidationFailsClosedAfterAmbiguousPublish','TestRecoveryHandoffRejectsLiveAndPublishingCoordinator','TestPhysicalReachabilityUnionEmptyDirectoriesOwnsLeasesAndRejectsLogicalAuthority','TestPhysicalReachabilityUnionFailedDirectoryRetainCleansOwnedLeases','TestRecoveryHandoffActualPhysicalCloseFailureIsSticky','TestConstructedPrimaryActivationRollbackPreservesCustodyV6']
sel['rootpublication']+=next(c['required_top_level_tests'] for c in json.loads((D/'native-main-integration/finaldriver-source-repair08/proposed-commands.json').read_text())['commands'] if c['name']=='normal-rootpublication-join')
sel['db']=['TestPrimarySourceOrdinaryBorrowerAfterDBCloseV6','TestPrimaryConstructionLedgerLateHandoffCustodyV6','TestPrimaryRuntimeDisposalRetainsOriginalQueueAndAccountingV6','TestPrimaryRuntimeDisposalPhysicalFailureIsOriginalAndStickyV6','TestOriginalStartupCleanupPhysicalRefusalRetainsLock','TestOriginalReadOnlyStartupCleanupPhysicalRefusalRetainsLock','TestPrimaryVacuumSharedArenaHeldSnapshotCutAndIndependentSlots','TestPrimaryJointVacuumV6SuccessfulReplacement','TestPrimaryJointVacuumV6PointersAndIndependentFallback','TestPrimaryJointVacuumV6OldRuntimeRetirementErrorRetainsPublishedPair','TestStableIndexResourceTokenOwnsVacuumFenceAfterSnapshotClose','TestPrimaryCapsuleOrdinaryAuthorityAndExactFallbackV6','TestPrimaryCapsulePostInstallFenceFailureRetainsCustodyV6','TestPrimaryCapsuleCapturedRecoverableReadRootsSurviveSlotReuseV6','TestNativeSnapshotBirthLookupAdmissionAndOriginalCleanupV6','TestNativeSnapshotPublicationUsesSameLeafOperandV6','TestNativeRootPreparationSourceWitnessV6','TestNativeRootPreparationSourceTerminalV6']
sel['db'] += [n for n,p in files.items() if p in ['TreeDB/db/recoverable_root_set_test.go','TreeDB/db/original_rootset_authority_completion_test.go']]
sel['mvcc']=['TestNativeFinaldriverEnvelopeCheckedArithmeticV6']
children['TestPrimaryReadRootSourceMixedOriginLastConsumerV6']=['ordinary-ordinary','ordinary-finite','native-ordinary','native-finite']
children['TestPrimarySourceOrdinaryBorrowerAfterDBCloseV6']=['snapshot','recoverable-set']
children['TestPrimaryVacuumSharedArenaHeldSnapshotCutAndIndependentSlots']=['manifest','directory']
children['TestPrimaryJointVacuumV6PointersAndIndependentFallback']=['false','true']
# Literal child labels are source-bound too, even when an old proposal omitted them.
for group,names in sel.items():
 for n in names:
  if n not in funcs: raise ValueError('missing top '+n)
  literals=re.findall(r'\bt\.Run\("([^"]+)"',funcs[n])
  if literals: children[n]=list(dict.fromkeys(children.get(n,[])+literals))
unknown=[]
for names in sel.values():
 for n in names:
  if re.search(r'\bt\.Run\((?!")',funcs[n]) and n not in children: unknown.append((n,files[n]))
print('UNRESOLVED_DYNAMIC',unknown)
env=next(c['env'] for c in old['commands'])
cmds=[]; packages={'retainedalloc':'internal/retainedalloc','primaryarena':'internal/primaryarena','rootpublication':'internal/rootpublication','memtable':'internal/memtable','lifecycle':'lifecycle','db':'db','mvcc':'mvcc'}
for mode in ['normal','race']:
 for group in ['mvcc','retainedalloc','lifecycle','memtable','primaryarena','rootpublication','db']:
  tops=list(dict.fromkeys(sel[group])); name=mode+'-'+group; e=copy.deepcopy(env);e['TMPDIR']=str(O/'controls'/name/'tmp')
  argv=[str(D/'toolchains/go1.26.3/bin/go'),'test','-mod=readonly','-p=1','-parallel=1','-count=1','-v','-timeout=3m']
  if mode=='race':argv+=['-race']
  argv+=['-run','^('+'|'.join(tops)+')$','./TreeDB/'+packages[group]]
  ch={n:children[n] for n in tops if n in children}
  cmds.append(dict(name=name,cwd=str(W),argv=argv,env=e,executed=False,selection_class='focused-source-guard-producer',outer_timeout_seconds=600,required_top_level_tests=tops,required_child_tests=ch,expected_top_level_count=len(tops),expected_child_count=sum(len(x) for x in ch.values())))
actual=json.loads((D/'native-main-integration/native-ledger-source04/proposed-commands.json').read_text())
for c in actual['commands']:
 if 'all-shared-arms' in c['name']:
  c=copy.deepcopy(c);c['name']=c['name'].replace('all-shared-arms','actual-all-shared-arms');c['selection_class']='actual-separate-selection-required';c['env']['TMPDIR']=str(O/'actual-controls'/c['name']/'tmp');cmds.append(c)
testbindings={files[n]:hashlib.sha256((W/files[n]).read_bytes()).hexdigest() for ns in sel.values() for n in ns}
proposal={'executed':False,'scope':'full producer guard changed-risk controls; actual arms separate selection','commands':cmds,'test_file_bindings':testbindings,'unresolved_dynamic_children':unknown}
(O/'proposed-commands.json').write_text(json.dumps(proposal,indent=2,sort_keys=True)+'\n')
print([(c['name'],c['expected_top_level_count'],c['expected_child_count']) for c in cmds[:14]])
