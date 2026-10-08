from pathlib import Path
import ast, datetime, hashlib, json, os, re, shlex, shutil, subprocess

helper = Path('/Users/michaelseiler/dev/snissn/gomap/tmp/mvcc-m7-root-append-integration-20261003T1510Z/record-ordinary-metadata-budget-root-20261008.py')
tree = ast.parse(helper.read_text())
allowed = {'OUT', 'LOCAL', 'REMOTE', 'D', 'END'}
parts = [n for n in tree.body if isinstance(n, (ast.Import, ast.ImportFrom, ast.FunctionDef)) or
         (isinstance(n, ast.Assign) and all(isinstance(t, ast.Name) and t.id in allowed for t in n.targets))]
ns = {}
exec(compile(ast.Module(body=parts, type_ignores=[]), str(helper), 'exec'), ns)
external = Path('/Volumes/FlashDrive/gomap-mvcc-4871-root-evidence-20261008')
ns['OUT'] = external
api, remote, immutable, sha = [ns[k] for k in ('api', 'remote', 'immutable', 'sha')]
local = external / 'graph-state.json'
remote_graph, decision_dir = ns['REMOTE'], ns['D']
expected = '7c005c9cc7f99e1aecdb093a2447052aeaf76aa907ea3ac924c52bf980ae0588'
assert sha(local.read_bytes()) == expected
assert remote('from pathlib import Path\nimport hashlib,json\nprint(json.dumps({"sha256":hashlib.sha256(Path('+repr(remote_graph)+').read_bytes()).hexdigest()}))')['sha256'] == expected
now = datetime.datetime.now(datetime.timezone.utc).isoformat()
stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')

code = r'''
import pathlib,json,hashlib,re
b=pathlib.Path('/home/mikers/benchmarks/mvcc-ordinary-5111-root-20261008')
expected={
 'ordinary-selected-producer-integration-first':('9726dfeecc75a303ecc22b0c843dfd4e4b57c611408528783ba7da4174188507','c268321fcc0be1f12cb71e2431418cd975877047efc836fccf33fc15a0cbb36e',10030,0,3,0),
 'ordinary-selected-producer-pointer-first':('8b26fd78575d28d3568333123ea268b767177052530eeca48ada7e2b8bbf300e','c05d3f806da1ed2affe4e3f742b1872ac379cc6bd4448143d7611eed1c62cd21',10030,0,4,0),
 'ordinary-owned-certification-fixture-fixed-red':('dcc67735974226c48e3262ac4017be9fdfd8c11376455b22e60e7162cd8cbacb','52f00448251f358d115a4d8eaf93db16327f2553b89cabc2ad2bf68dc3690eef',10030,1,0,1),
 'ordinary-owned-certification-first':('0f79a152ae3285064c77f72921d5980ae8a715ddefa81be1117607d12c5e6079','50ef3662130ea4f45d7557d36878759e0c98d7c332c256f2cef311bacbf2141f',10031,0,2,0)}
keys=['TreeDB/db/durable_root_runtime.go','TreeDB/db/primary_capsule_runtime_v6.go','TreeDB/db/db.go','TreeDB/db/stable_pager_owned_operations.go','TreeDB/internal/valuelog/stable_resource.go','TreeDB/internal/rootpublication/resource_set.go','TreeDB/internal/rootpublication/resource_builder_allocation.go','TreeDB/internal/rootpublication/resource_kind_allocation.go','TreeDB/internal/rootpublication/resource_import_set.go','TreeDB/internal/rootpublication/resource_import_directory.go','TreeDB/internal/rootpublication/resource_requirements_allocation.go','TreeDB/internal/rootpublication/resource_requirements_allocation_test.go','TreeDB/internal/rootpublication/resource_mutation_allocation.go','TreeDB/internal/rootpublication/resource_append_owned_merge.go','TreeDB/caching/primary_metadata_custody_test.go']
out={}
for name,(rawsha,mapsha,count,exitcode,npass,nfail) in expected.items():
 p=b/name; raw=(p/'raw.log').read_bytes(); before=(p/'source-before.json').read_bytes(); after=(p/'source-after.json').read_bytes(); r=json.loads((p/'result.json').read_bytes()); m=json.loads(before)
 h=lambda x:hashlib.sha256(x).hexdigest()
 assert h(raw)==rawsha==r['raw_sha256'] and before==after and h(before)==mapsha==r['before_sha256']==r['after_sha256']
 assert len(m)==count==r['source_count'] and r['source_equal'] and r['exit']==exitcode
 assert r['head']=='2d6b07f58902537cfe8d2b3e3a8c3c9ba0f3c07c' and r['source']==str(b/'source')
 assert r['env']['GOWORK']=='off' and r['env']['GOTOOLCHAIN']=='local' and r['env']['GOROOT'].endswith('go1.26.4.linux-amd64')
 assert r['args'][0]=='test' and '-count=1' in r['args'] and '-race' not in r['args']
 text=raw.decode(); passes=re.findall(r'^--- PASS: (\S+)',text,re.M); fails=re.findall(r'^--- FAIL: (\S+)',text,re.M); skips=re.findall(r'^--- SKIP: (\S+)',text,re.M)
 assert (len(passes),len(fails),len(skips))==(npass,nfail,0)
 out[name]={'directory':str(p),'result':r,'result_sha256':h((p/'result.json').read_bytes()),'actual_top_level_passes':passes,'actual_top_level_failures':fails,'actual_top_level_skips':skips,'no_matching_test_packages':re.findall(r'^ok\s+(\S+).*\[no tests to run\]',text,re.M),'relevant_source_bindings':{k:m[k] for k in keys if k in m},'current_binding_matches':{k:h((b/'source'/k).read_bytes())==m[k] for k in keys if k in m and (b/'source'/k).is_file()}}
assert out['ordinary-owned-certification-first']['actual_top_level_passes']==['TestOwnedMutationCertificationAndAppendMerge','TestPrimaryCapsuleOrdinaryAuthorityAndExactFallbackV6']
assert out['ordinary-selected-producer-pointer-first']['actual_top_level_passes']==['TestPrimaryCapsulePostInstallFenceFailureRetainsCustodyV6','TestPrimaryDirectoryPhysicalBaseValueLogRetention','TestPrimaryOwnedPromotionV6DoesNotGrowAdmittedReaderClosure','TestCOWDictionaryPrimaryTemporaryCaptureDetachesGovernor']
print(json.dumps(out))
'''
packets = json.loads(subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','mikers@192.168.0.185','python3 -c '+shlex.quote(code)],capture_output=True,text=True,check=True).stdout)
component = immutable('ordinary-production-certification-components-root-'+stamp+'.json', {
 'at_utc':now,'classification':'SOURCE_BOUND_NORMAL_PRODUCTION_AND_CERTIFICATION_COMPONENTS_ONLY',
 'packets':packets,'tests_rerun_by_root':False,'performance_qualified':False,'dependency_ready':False,
 'source_audit':'Root inspected production pre-first-Clone import, fresh VLog MetadataOwner, actual Snapshot completion, selected requirement/mutation helpers and direct builder selection. Removed metadata guards no longer force append/full requirements fallback. Selected borrowed immutable summaries and admitted duplicate-membership scratch are used. No allocation census or wrapper claim certifies complete physical cleanup.',
 'remaining':'Selected same-identity collision union/rebuild history scaling, original cleanup completion versus consumed refs/debt/no replay, all19 production allocation/lifetime closure, normal/race validation, docs/CI impact, local product freeze, matched storage/retention/performance, mature PR/current-head review/CI/final-base and root merge.',
 'foreign_R1':'Owner reports230 normal passes including repaired fixtures; race running on .111. Root has not accepted final raw packet or adopted49fe06 ABI proposal. No blanket dependency or external caching mutation grant.',
 'released_paths':[]})

issue = api('repos/snissn/gomap/issues/5111')
issue_body = (helper.parent/'m7-ordinary-prerequisite-root-20261008/ordinary-prerequisite-body.md').read_text()
assert issue['state']=='open' and issue['body']==issue_body
comment = api('repos/snissn/gomap/issues/comments/6054393776')
body = comment['body']
assert body.startswith('<!-- codex-issue-graph-executor:ordinary-focused-checkpoint -->')
backup = ns['immutable_bytes']('ordinary-focused-comment-before-production-certification-root-'+stamp+'.md',body.encode())
def paragraph(value, heading, text):
 start=value.index(heading); end=value.index('\n\n',start)
 return value[:start]+text+value[end:]
body = paragraph(body,'**Selected import construction verified:**','**Selected import construction verified:** root checked six actual race cases (`2bbbcd0f`, equal10028-path map `e315b35f`): original-operation custody, real identity collisions/exact RID union, partial capacity refusal, shared directories and empty-directory leases. One preadmitted flattened canonical directory per exact root is reused for sizing/copy. Seven requirements race cases pass (`92c56e0d`, equal10029-path map `3ced4886`); subsequent production integration repairs the retained unscoped-nonempty error-class RED and passes parity. Root did not rerun tests. Void foreign cohort release proves adapter detachment only; physical/namespace/debt outcomes remain with their original owner. Cohort retention and later source remain unqualified.')
production = '**Production and certification components verified:** root checked3 actual normal integration passes (`9726dfee`, equal10030-path map `c268321f`), covering scope/error parity and real capsule fallback; value-log in that packet is compile-only. A second packet passes4 actual normal pointer/old-reader/post-install uncertainty/dictionary-after-capture cases (`8b26fd78`, equal10030-path map `c05d3f80`). Root found selected metadata guards that forced exact fallback and bypassed mutation certification. The worker removed them and selects one builder directly. Retained selected destructive-transfer RED `dcc67735` is followed by2 actual normal passes (`0f79a152`, equal10031-path map `50ef3662`): admitted append certification/transfer, complete deletion/filter/refusal and capsule fallback. Root read the selected immutable summary/membership and admitted scratch code. No tests were rerun; these source-bound components do not certify final race, complete19 closure or performance. Same-identity union/rebuild history scaling remains to resolve before freeze.'
anchor='**Adjacent completion coordination:**'
assert '**Production and certification components verified:**' not in body
body=body.replace(anchor,production+'\n\n'+anchor,1)
body=paragraph(body,anchor,'**Adjacent completion coordination:** the retained R1 scalar completion/debt ABI is a proposal, without adoption. Its owner now reports230 exactGo1.26.3 normal passes including the two repaired fixtures; race is running separately on .111. Root has not accepted that new full raw packet. Exact surviving debt authority remains distinct from consumed refs. No blanket dependency, member-local ABI acceptance or external caching edit is granted; one exact original completion/debt reconciliation remains before #5111 readiness.')
body=paragraph(body,'**Remaining coherent ownership work:**','**Remaining coherent ownership work:** the selected production import/new-provider path has the normal component evidence above, while later source, same-identity union scaling, actual consumed VLog reference plus namespace debt, failed Close/Unobserve and full provider closure remain unqualified. Finish all19 allocation/lifetime groups together, preserving independent proof lifetime, generic diagnostics and no double release/refund. Then freeze locally before matched storage/allocation/retention performance and mature PR gates. No new issue, blanket R1 dependency or native activation.')
free=shutil.disk_usage('/Users/michaelseiler/dev/snissn/gomap').free
body=paragraph(body,'**Host/cleanup:**','**Host/cleanup:** the sole ordinary source/focused Go owner continues on .185. Native4878 source/evidence on .111 remains frozen; separately owned functional work retains its own space/ownership limits. The prior released graph cache is verified absent; the latest bounded cleanup found no new eligible target. At14:11 UTC local Mac writes failed even at2KiB. A foreign owner subsequently reported deletion of its own18 released successful binaries; root has not adopted its deletion receipt. Current Mac free bytes are'+str(free)+' at this checkpoint. New root receipts and the current graph mirror remain on the mounted FlashDrive; remote source/evidence remains usable. Protected source/raw/maps/negative packets are retained. No new graph cleanup or runtime/source grant.')
body += '\n\nProduction/certification checkpoint: '+now+'. Immutable component receipt `'+Path(component['local']).name+'` (`'+component['sha256']+'`). Existing issue body/all19 gates and every native/M3/M8 dependency remain unchanged.\n'
assert api('repos/snissn/gomap/issues/comments/6054393776')['body']==comment['body']
api('repos/snissn/gomap/issues/comments/6054393776',{'body':body})
live=api('repos/snissn/gomap/issues/comments/6054393776')
assert live['body']==body and api('repos/snissn/gomap/issues/5111')['body']==issue_body
live_receipt=immutable('ordinary-production-certification-live-root-'+stamp+'.json',{'at_utc':now,'comment_url':live['html_url'],'comment_sha256':sha(body.encode()),'prior_comment':backup,'issue_body_preserved':True,'issue_body_sha256':sha(issue_body.encode()),'component':component,'dependency_ready':False})

g=json.loads(local.read_bytes())
n=next(x for x in g['nodes'] if x['name']=='M7P')
others=json.dumps([x for x in g['nodes'] if x is not n],sort_keys=True)
c=dict(n['current_checkpoint'])
c.update({'at_utc':now,'phase':'selected production and admitted certification connected; same-identity history scaling and all19 completion closure before freeze','production_and_certification_components':component,'live_progress_receipt':live_receipt,'tests_rerun_by_root':False,'dependency_ready':False,'remaining_gate':'Same-identity history scaling, exact original completion/debt, all19 production closure, affected normal/race/docs/CI impact, local product freeze then matched qualification and mature PR/root gates.'})
n['current_checkpoint']=c
n['dependency_ready']=False
g['m7_ordinary_extraction_current_checkpoint_20261008']=c
g['m7_ordinary_production_certification_frontier_20261008']={'component':component,'live_receipt':live_receipt,'dependency_ready':False}
g['updated_utc']=now
assert others==json.dumps([x for x in g['nodes'] if x is not n],sort_keys=True)
new=(json.dumps(g,indent=2)+'\n').encode(); h=sha(new)
assert sha(local.read_bytes())==expected
code='from pathlib import Path\nimport sys,hashlib,json,os\np=Path('+repr(remote_graph)+')\nb=sys.stdin.read().encode()\nassert hashlib.sha256(p.read_bytes()).hexdigest()=='+repr(expected)+'\njson.loads(b)\nq=p.with_name('+repr('graph-state.production-certification-'+stamp+'.pending.json')+')\nwith q.open("xb") as f:f.write(b);f.flush();os.fsync(f.fileno())\nassert hashlib.sha256(q.read_bytes()).hexdigest()=='+repr(h)+'\nassert hashlib.sha256(p.read_bytes()).hexdigest()=='+repr(expected)+'\nos.replace(q,p)\nprint(json.dumps({"sha256":hashlib.sha256(p.read_bytes()).hexdigest()}))'
assert remote(code,new.decode())['sha256']==h
pending=local.with_name('graph-state.production-certification-'+stamp+'.pending.json')
with pending.open('xb') as f:f.write(new);f.flush();os.fsync(f.fileno())
assert sha(local.read_bytes())==expected
os.replace(pending,local)
assert sha(local.read_bytes())==h
receipt=immutable('ordinary-production-certification-graph-root-'+stamp+'.json',{'at_utc':now,'old_graph_sha256':expected,'new_graph_sha256':h,'external_remote_equal':True,'unrelated_nodes_preserved':True,'component':component,'live_receipt':live_receipt,'dependency_ready':False,'released_paths':[]})
print(json.dumps({'component':component,'receipt':receipt,'graph_sha256':h,'comment_url':live['html_url'],'dependency_ready':False}))
