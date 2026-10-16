from pathlib import Path
import copy, hashlib, json, re
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'native-integrated';A=D/'native-main-integration/index-observer-inventory-final'
P=D/'native-main-integration/index-observer-repair/proposed-focused-controls.json';M=D/'native-decision/root-index-observer-controls-selection.md'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
assert sha(A/'source-final.json')=='21bc95e755d65748083193171e2ba2ed4f0d72454189ac2e61b087e998d5acd3'
source=json.loads((A/'source-final.json').read_text());proposal=json.loads(P.read_text());commands=[];bindings={};children={}
for p in proposal['commands']:
 c=copy.deepcopy(p);c['id']=c.pop('name');c['top_level_tests']=c.pop('required_top_level_tests');c['race']='-race' in c['argv'];c['package']=c['argv'][-1];c['selector']=c['argv'][c['argv'].index('-run')+1]
 c['candidate_manifest_sha256']=sha(A/'source-final.json');c['status']='AUTHORIZED_ONCE_BY_ROOT';c['env']['TMPDIR']=str(D/'native-main-integration/index-observer-controls'/c['id']/'tmp')
 actual=[]
 for q in (W/c['package'][2:]).glob('*_test.go'):
  for name in re.findall(r'^func (Test\w+)\(t \*testing\.T\)',q.read_text(),re.M):
   if re.search(c['selector'],name):
    actual.append(name);rel=str(q.relative_to(W));assert sha(q)==source[rel];bindings[rel]=source[rel]
 assert sorted(actual)==sorted(c['top_level_tests'])
 for child in c.pop('required_child_tests'):
  parent,sub=child.split('/',1);assert parent in actual
  if sub not in children.setdefault(parent,[]):children[parent].append(sub)
 commands.append(c)
assert [c['race'] for c in commands]==[False,False,True,True]
assert sum(len(c['top_level_tests']) for c in commands if not c['race'])==7
assert sum(len(v) for v in children.values())==28
M.write_text('''# Root selection: original index capture observer controls

The six-path observer correction is independently source ACCEPT on9d6c8de3/archive72eb1c83. Root has reviewed the full delta, both real-capture/backing controls, observer pricing and actual compile receipt. It replaces an invalid equality between distinct original/duplicate File wrappers with full immutable physical identity, original backing, owner, registry, pin and selected pager checks for both DATA and PRIMARY. Pending cleanup custody remains present and incomplete. No production lifetime, layout, F293/R145, cap or authority changed.

Root staged the six selected files and refreshed only CI discovery metadata. Finalsource21bc95e755d65748083193171e2ba2ed4f0d72454189ac2e61b087e998d5acd3/archive00e91ecda5dfea9925f4386084bf33168b67b0d28441c31b420b8563cff832c7/treeaafeb5f94f4c3e3ff249bd31728720d9c942649c has10377files and204stagedpaths. Source-contract PASS. The prior58Python checks and retained-file pager normal/race results remain separately attributed under their explicit source-invariance receipts; no rerun implied.

Authorize exactly the four JSON commands once, sequentially: normalDB, normalrootpublication, raceDB, racerootpublication. Each mode must observe and PASS all7top-level names and28explicit children. These exercise genuine capture/duplicate/backing construction, bounded refusal, partial construction/cancellation and original physical cleanup; no native publication or actual33 is included. Fixed pinnedofflineGo1.26.3, p1/parallel1/count1, Go3m, outer300snormal/360srace, GOMAXPROCS2/GOMEMLIMIT8GiB. No source or index mutation.

Native_prerequisite is sole runtime operator, exactgpt-6.1-sol/no children. Root retains all Git/index/GitHub. Evidence goes to native-main-integration/index-observer-controls. Bind source/archive/index/tool/compiler/argv/environment before launch, every raw RUN/PASS and actual PID/PGID/subreaper/join. Stop first failure, skip, missing/unexpected test, source drift, timeout, intervention or process residue before any remaining call, fix or retry. Return partial evidence and release runtime ownership. Raw executed metadata must reflect actual launch, with proposal status preserved separately.

This is scoped observer-control evidence only. Actual33 requires a separate root selection after acceptance. Original Q32/1MiB/33/80/4096 remain RED/ACK0. Full retention/private backing, transfer dedup, nonempty routes, real native Finish, public C1/C3 and final graph qualification remain open. No numeric allowance selected.
''')
x=dict(selection_sha256=sha(M),proposal_sha256=sha(P),source_manifest=str(A/'source-final.json'),source_manifest_sha256=sha(A/'source-final.json'),source_archive=str(A/'source-final.tar.gz'),source_archive_sha256='00e91ecda5dfea9925f4386084bf33168b67b0d28441c31b420b8563cff832c7',source_files=len(source),source_tree='aafeb5f94f4c3e3ff249bd31728720d9c942649c',source_mutations_allowed=False,actual33_authorized=False,native_publication_authorized=False,normal_top_level_tests=7,race_top_level_tests=7,selected_test_files=bindings,required_child_tests=children,commands=commands,stop_at_first_failure=True)
M.with_suffix('.json').write_text(json.dumps(x,indent=2,sort_keys=True)+'\n')
print(json.dumps(dict(selection_md_sha256=sha(M),selection_json_sha256=sha(M.with_suffix('.json')),calls=len(commands),source_files=len(source),test_files=len(bindings),children_each_mode=28),indent=2))
