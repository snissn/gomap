from pathlib import Path
import hashlib,json,tarfile,stat,difflib,re,os
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'native-integrated';O=D/'native-main-integration/native-registry-api-preparation';P=D/'native-main-integration/native-ledger-source04'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def put(n,d): (O/n).write_text(json.dumps(d,indent=2,sort_keys=True)+'\n')
base=json.loads((P/'source-frozen.json').read_text());bm=json.loads((P/'source-modes.json').read_text())
paths=sorted(p for root in ['TreeDB/db','TreeDB/lifecycle'] for p in (W/root).rglob('*') if p.is_file())
m={str(p.relative_to(W)):sha(p) for p in paths};modes={str(p.relative_to(W)):oct(stat.S_IMODE(p.stat().st_mode)) for p in paths}
put('owned-source.json',m);put('owned-modes.json',modes)
changed={p:{'before_sha256':base.get(p),'after_sha256':h,'before_mode':bm.get(p),'after_mode':modes[p]} for p,h in m.items() if h!=base.get(p) or modes[p]!=bm.get(p)}
deleted=[p for p in base if (p.startswith('TreeDB/db/') or p.startswith('TreeDB/lifecycle/')) and p not in m]
assert not deleted,deleted
put('changed-core.json',changed)
for name,names in [('owned-source.tar.gz',list(m)),('changed-core.tar.gz',list(changed))]:
 with tarfile.open(O/name,'w:gz') as t:
  for n in names:t.add(W/n,arcname=n,recursive=False)
old={}
with tarfile.open(P/'source-frozen.tar.gz','r:gz') as t:
 for e in t:
  n=e.name.removeprefix('./')
  if n in changed and e.isfile():old[n]=t.extractfile(e).read()
patch=[]
for n in changed:
 a=old.get(n,b'').decode(errors='replace').splitlines(keepends=True);b=(W/n).read_text().splitlines(keepends=True)
 patch.extend(difflib.unified_diff(a,b,fromfile=('a/'+n if n in old else '/dev/null'),tofile='b/'+n))
(O/'owned-core.patch').write_text(''.join(patch))
# Include all actual test function bindings affected by this worker; targeted selectors below are narrower.
tests={}
for n in changed:
 if n.endswith('_test.go'):
  text=(W/n).read_text();tests[n]={'sha256':m[n],'top_level_tests':re.findall(r'^func (Test\w+)\(',text,re.M),'literal_children':re.findall(r'\bt\.Run\("([^"\n]+)"',text)}
put('changed-test-bindings.json',tests)
life=sorted(set(re.findall(r'^func (Test\w+)\(',''.join(p.read_text() for p in (W/'TreeDB/lifecycle').glob('*_test.go')),re.M))-{'TestMain'})
db=[]
files=['reader_registry_lifetime_v6_test.go','native_snapshot_minimum_admission_v6_test.go','native_snapshot_minimum_interleaving_v6_test.go','snapshot_manager_cleanup_test.go','native_generation_containing_completion_test.go','snapshot_primary_retention_test.go','native_data_dependency_release_work_v6_test.go']
for f in files:db+=re.findall(r'^func (Test\w+)\(',(W/'TreeDB/db'/f).read_text(),re.M)
db+=['TestPrimaryOfflineScopeV6ParentOutlivesPhysicalPair','TestPrimaryOfflineScopeV6PhysicalFailureRetainsControls','TestPrimaryOfflineScopeV6ParentFailureRetainsExactCustody']
commands=[]
for race in [False,True]:
 for pkg,names in [('lifecycle',life),('db',db)]:
  name=('race-' if race else 'normal-')+pkg;env={'GOTOOLCHAIN':'local','GOPROXY':'off','GOSUMDB':'off','GOMAXPROCS':'2','TMPDIR':str(O/'proposed-controls'/name/'tmp')}
  argv=[str(D/'toolchains/go1.26.3/bin/go'),'test','-p=1','-parallel=1','-count=1','-v','-timeout=3m']+(['-race'] if race else [])+['./TreeDB/'+pkg,'-run','^('+'|'.join(names)+')$']
  commands.append({'name':name,'executed':False,'cwd':str(W),'argv':argv,'env':env,'outer_timeout_seconds':600,'required_top_level_tests':names})
put('proposed-commands.json',{'executed':False,'scope':'proposal only; root must bind combined source/tool/env/process grant before execution','compile_proposal':{'executed':False,'argv':[str(D/'toolchains/go1.26.3/bin/go'),'test','-run','^$','-p=1','./...'],'note':'Root prepared combined compile-only runner is authoritative; this is no execution authorization.'},'commands':commands,'required_dynamic_children':{'TestReaderRegistryPositiveCompletionV6':['nil-work','short-work','different-owner'],'TestNativeDataDependencyReleaseRechecksCurrentGenerationV6':['current','retired']}})
assert all(sha(W/n)==h for n,h in m.items())
receipt={'owned_files':len(m),'changed_files':len(changed),'changed_production_files':sum(not n.endswith('_test.go') for n in changed),'changed_test_files':len(tests),'deleted_files':deleted,'parent_manifest_sha256':sha(P/'source-frozen.json'),'parent_archive_sha256':sha(P/'source-frozen.tar.gz'),'source_stable_after_archives':True,'source_lease':'released upon final handoff','runtime_lease':'none; released','compiler_or_tests_executed':False,'raw_index_sha256':sha(D/'native-integrated/.git') if (D/'native-integrated/.git').is_dir() else None}
# .git is a worktree pointer, read actual index directly without invoking Git.
gitline=(W/'.git').read_text().strip();gd=Path(gitline.split(': ',1)[1]);receipt['raw_index_sha256']=sha(gd/'index')
put('seal-receipt.json',receipt)
print(json.dumps(receipt,indent=2));print('MANIFEST',sha(O/'owned-source.json'));print('ARCHIVE',sha(O/'owned-source.tar.gz'));print('DELTA',sha(O/'changed-core.json'));print('PROPOSAL',sha(O/'proposed-commands.json'))
