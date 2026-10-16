from pathlib import Path
import copy,hashlib,json,re
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'native-integrated';A=D/'native-main-integration/fixture-inventory-final';P=D/'native-decision/root-origin-integrated-controls-selection.json';M=D/'native-decision/root-fixture-integrated-controls-selection.md'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
assert sha(P)=='9aa62ed39b31d92fb238fa1251facf85d567d16c84c9e5781350000710eb7331'
assert sha(A/'source-final.json')=='041b0ddfca3581c2b773ceb857a27b2dd4c718775b45a9cb71d0c7cc3107744d'
source=json.loads((A/'source-final.json').read_text());p=json.loads(P.read_text());commands=copy.deepcopy(p['commands'][1:]);assert len(commands)==18
c=commands[0];assert c['id']=='normal-TreeDB-internal-rootpublication';c['selector']='^TestPrimaryFinishResourceRefusalAndFailureCustodyV6$';c['argv'][c['argv'].index('-run')+1]=c['selector'];c['top_level_tests']=['TestPrimaryFinishResourceRefusalAndFailureCustodyV6'];c['id']='normal-repaired-resource-finish'
bindings={}
for c in commands:
 c['candidate_manifest_sha256']=sha(A/'source-final.json');c['status']='AUTHORIZED_ONCE_BY_ROOT';c['executed']=False
 for n in c['top_level_tests']:
  files=[q for q in (W/c['package'][2:]).glob('*_test.go') if re.search(r'^func '+n+r'\(',q.read_text(),re.M)];assert len(files)==1
  rel=str(files[0].relative_to(W));assert sha(files[0])==source[rel];bindings[rel]=source[rel]
assert [c['race'] for c in commands]==[False]*9+[True]*9
normal=sum(len(c['top_level_tests']) for c in commands if not c['race']);race=sum(len(c['top_level_tests']) for c in commands if c['race']);assert (normal,race)==(19,63)
text='''# Root selection: corrected fixture and remaining integrated controls

Root accepts the one-function source correction selected a83b2072 after complete function/source review and independent exactSol source review. Frozen a7ff76f8/archive165f6284 has10375 files; all production and all same-file text outside TestPrimaryFinishResourceRefusalAndFailureCustodyV6 are identical to e434542a. It preserves3 as lookup refusal, adds5 for callback refusal, retains11 and two fresh32 turns with exact31/12 consumed records, original construction/Set/counters/terminal charge and sticky first physical failure. No production prices/layouts/caps changed, no new native allowance. This synthetic fixture is not real native publication/Finish qualification.

Root alone staged the selected test and refreshed only CI discovery_source_sha256. Final manifest041b0ddfca3581c2b773ceb857a27b2dd4c718775b45a9cb71d0c7cc3107744d/archive963f9deb573465a5e56a9e2d94841058851bbc2ae28e01cc7533e7cee7106182/stagedtree2fc987a8d295af50286fa81d103537e569466eda has202staged paths. Source contract passes. Prior58Python checks are reused: scripts/tests and relevant policy semantics are byte-identical, receipts bound in fixture-inventory-final. No redundant full compile or Python-suite rerun. Source archival and live/index binding required before launch.

Authorize ONE sequential18-call sequence in JSON: normal corrected test alone, then17 previously unexecuted commands. Nine normal/nine race calls select19normal/63race top-level names. Prior normalDB31top/75all and other16rootpublication normal passes remain source-applicable and are not repeated. Failed predecessor packet is immutable and superseded only for the corrected test. All race controls remain required, including full DB and rootpublication selections. Preserve the exact selected M4 pointer/backend-vacuum-online cell.

Native_prerequisite is sole operator, exactgpt-6.1-sol/no children, frozen native-integrated worktree; root owns Git/index/GH. Evidence native-main-integration/fixture-controls. Use exact pinnedGo1.26.3/offline environment, p1/parallel1/count1 and existing Go/outer deadlines, all normal before race. No source edits. Every named top/required child and every observed RUN must PASS, no SKIP; one exact M4 cell. Bind raw argv/env/tool/compiler/source before-after/actual PIDPGID and subreaper/join. Stop first failure, missing name, skip, drift, timeout, leak or intervention BEFORE any retry/fix/remaining command. Return partial evidence and runtime lease; no passing-row search.

Allowed scope is unchanged from d86a94d8: real ordinary publication/read/Close/reopen/cache/vacuum; finite native capture/cancel/source-witness/unfinished-receipt refusal, standalone pager sync/file-close and explicitly named owner/queue/callback synthetic primitives. No actual33/final native driver, real native DB activation/publication/fence/Consume/Finish, broad benchmark or allowance. F293/R145 remain incomplete source expressions; full retained/private backing, future Finish, public C1/C3, Q32/1MiB/33/80/4096 remain RED/ACK0 and graph qualification open.
'''
M.write_text(text)
x=dict(selection_sha256=sha(M),predecessor_selection_sha256=sha(P),source_manifest=str(A/'source-final.json'),source_manifest_sha256=sha(A/'source-final.json'),source_archive=str(A/'source-final.tar.gz'),source_archive_sha256='963f9deb573465a5e56a9e2d94841058851bbc2ae28e01cc7533e7cee7106182',source_files=len(source),source_tree='2fc987a8d295af50286fa81d103537e569466eda',source_mutations_allowed=False,actual33_authorized=False,native_publication_authorized=False,synthetic_native_callback_controls_authorized=True,normal_top_level_tests=normal,race_top_level_tests=race,selected_test_files=bindings,required_child_tests=p['required_child_tests'],commands=commands,stop_at_first_failure=True)
M.with_suffix('.json').write_text(json.dumps(x,indent=2,sort_keys=True)+'\n');print(json.dumps(dict(selection_md_sha256=sha(M),selection_json_sha256=sha(M.with_suffix('.json')),calls=len(commands),normal=normal,race=race,test_files=len(bindings)),indent=2))
