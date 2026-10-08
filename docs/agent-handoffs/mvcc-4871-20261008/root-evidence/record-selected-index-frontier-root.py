from pathlib import Path
import ast, datetime, hashlib, json, os, re, shlex, subprocess

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
remote_graph = ns['REMOTE']
expected = '2409ef03fc3690edfce59f90803e08e01b49e12588e40cb385e975f52e30a9d2'
assert sha(local.read_bytes()) == expected
assert remote('from pathlib import Path\nimport hashlib,json\nprint(json.dumps({"sha256":hashlib.sha256(Path('+repr(remote_graph)+').read_bytes()).hexdigest()}))')['sha256'] == expected
now = datetime.datetime.now(datetime.timezone.utc).isoformat()
stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')

code = r'''
import pathlib,json,hashlib,re
b=pathlib.Path('/home/mikers/benchmarks/mvcc-ordinary-5111-root-20261008')
p=b/'ordinary-selected-fresh-rid-first'
h=lambda x:hashlib.sha256(x).hexdigest()
raw=(p/'raw.log').read_bytes(); before=(p/'source-before.json').read_bytes(); after=(p/'source-after.json').read_bytes()
r=json.loads((p/'result.json').read_bytes()); m=json.loads(before)
assert h(raw)==r['raw_sha256']=='1f2a098933cf66ed894adfcdb59bdda823479703c267764ffaca5e6867e832e4'
assert before==after and h(before)==r['before_sha256']==r['after_sha256']=='b439adda0d60c19a4b87357c93c0beff14ea0f6c9b8aeb215c5f9e2147a71046'
assert r['source_equal'] and r['exit']==0 and len(m)==r['source_count']==10031
assert r['source']==str(b/'source') and r['head']=='2d6b07f58902537cfe8d2b3e3a8c3c9ba0f3c07c'
assert r['env']['GOWORK']=='off' and r['env']['GOTOOLCHAIN']=='local' and r['env']['GOROOT'].endswith('go1.26.4.linux-amd64')
assert '-race' in r['args'] and '-count=1' in r['args'] and r['args'][0]=='test'
t=raw.decode(); passes=re.findall(r'^--- PASS: (\S+)',t,re.M); fails=re.findall(r'^--- FAIL: (\S+)',t,re.M); skips=re.findall(r'^--- SKIP: (\S+)',t,re.M)
assert passes==['TestStableLogicalObligationCommitmentCertifiesOnlyCompleteMutation','TestStableLogicalObligationMutationRequiresExactFinalRequirements','TestStableLogicalObligationMutationFinalRequirementsSearchesWithinFieldGroup','TestOwnedMutationCertificationAndAppendMerge','TestSelectedStableValueLogCaptureOwnsExactRIDAndRefusesBeforePin'] and not fails and not skips
keys=['TreeDB/internal/valuelog/stable_resource.go','TreeDB/internal/valuelog/stable_resource_test.go','TreeDB/internal/rootpublication/resource_mutation_allocation.go','TreeDB/internal/rootpublication/resource_requirements_allocation_test.go','TreeDB/internal/rootpublication/resource_append_owned_merge.go','TreeDB/internal/rootpublication/resource_set.go','TreeDB/internal/rootpublication/resource_coalesce_allocation.go','TreeDB/internal/rootpublication/resource_entry_outgoing_allocation.go']
out={'directory':str(p),'result':r,'result_sha256':h((p/'result.json').read_bytes()),'actual_top_level_passes':passes,'actual_top_level_failures':fails,'actual_top_level_skips':skips,'no_matching_test_packages':re.findall(r'^ok\s+(\S+).*\[no tests to run\]',t,re.M),'source_bindings':{k:m[k] for k in keys if k in m},'current_binding_matches':{k:h((b/'source'/k).read_bytes())==m[k] for k in keys if k in m and (b/'source'/k).is_file()}}
audited={'TreeDB/internal/rootpublication/resource_kind_merge_allocation.go':'bdb46ea193f94768ae1fab65ef83885fbc048c8a2cf46613eb36126235fad851','TreeDB/internal/rootpublication/resource_coalesce_allocation.go':'782411d75853e27940c18fcb50e1642c8f4cb7ddc21e986a9ee096344974adae','TreeDB/internal/rootpublication/resource_entry_outgoing_allocation.go':'0a298c11ae123fefb9586979763bbbf764e09516a02f567d904c053d82e170e8'}
out['root_audit_source_bindings']=audited
out['root_audit_current_matches']={k:h((b/'source'/k).read_bytes())==v for k,v in audited.items()}
print(json.dumps(out))
'''
packet = json.loads(subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','mikers@192.168.0.185','python3 -c '+shlex.quote(code)],capture_output=True,text=True,check=True).stdout)
component = immutable('ordinary-selected-rid-race-component-root-'+stamp+'.json',{
    'at_utc':now,'classification':'SOURCE_BOUND_FIVE_ACTUAL_RACE_COMPONENT_PASSES',
    'packet':packet,'tests_rerun_by_root':False,'dependency_ready':False,'performance_qualified':False,
    'root_test_audit':'Exact RID input is detached from caller mutation, source set/clone lifetime remains charged, last clone releases metadata, closed admission refuses capture without consuming manager file. This bounded component does not certify namespace-debt completion or all production providers.',
    'source_applicability':'Recorded before/after map is exact. Current matches are diagnostic only; history representation is changing, so reuse each original binding without relabeling these as final-candidate checks.',
    'released_paths':[]})
decision = immutable('ordinary-selected-persistent-index-decision-root-'+stamp+'.json',{
    'at_utc':now,'issue':'https://github.com/snissn/gomap/issues/5111','owner':'/root',
    'implementation_owner':'/root/m7_primary_directory_recovery_sol61','model':'gpt-6.1-sol',
    'source_bindings':packet['root_audit_source_bindings'],
    'finding':'Same-identity selected collision enters rebuildOwnedResourceKindCollision; cloneOwnedResourceEntryUnion acquires complete left/right history, combines exact obligations, and reconstructs indexes. This replays old retained history for small additions. Its elapsed-time contribution is unmeasured.',
    'decision':'Within the same all19 correction, use existing admitted outgoing.index as canonical complete immutable history, with optional constructor/delta obligation backing. Share same-owner persistent nodes and append only actual delta where certified; each node preserves the exact admitted backing and original physical source custody. Update import, filter, manifest, certification and diagnostics consumers together. Exact serialization or cross-owner import may traverse when required.',
    'preserved_gates':['Duplicate/conflict/namespace/RID parity','Original cleanup completion, consumed-reference distinction and surviving debt without replay','All19 actual producer admission and synchronized lifetime','Normal/race closure and canonical docs/CI impact','Product freeze before matched storage/allocation/retention performance','Independent review/current-head CI/final-base and root merge'],
    'new_issues':[],'new_owners':[],'edge_changes':[],'native_activation':False,'dependency_ready':False,
    'status':'COHERENT_HISTORY_REPRESENTATION_CORRECTION_IN_PROGRESS_NOT_PERFORMANCE_ACCEPTANCE'})

issue=api('repos/snissn/gomap/issues/5111')
issue_body=(helper.parent/'m7-ordinary-prerequisite-root-20261008/ordinary-prerequisite-body.md').read_text()
assert issue['state']=='open' and issue['body']==issue_body
comment=api('repos/snissn/gomap/issues/comments/6054393776'); body=comment['body']
assert body.startswith('<!-- codex-issue-graph-executor:ordinary-focused-checkpoint -->')
backup=ns['immutable_bytes']('ordinary-focused-comment-before-selected-index-root-'+stamp+'.md',body.encode())
anchor='**Adjacent completion coordination:**'
assert '**Selected RID race and history representation:**' not in body
addition='**Selected RID race and history representation:** root verified5 actual race passes (`1f2a0989`, equal10031-path map `b439adda`) for exact caller-independent RID capture, closed-admission refusal, complete obligation mutation and selected append transfer. No tests were rerun. These are source-bound components while the history representation changes. Root confirmed that same-identity collision can reconstruct complete old obligation history. The sole worker is reusing the existing admitted persistent index as canonical history, with only constructor/delta backing where needed; imports, filters, manifests, certification and diagnostics must use that representation coherently. Duplicate/conflict/namespace/RID parity and actual original cleanup custody remain required. Same-identity scaling and all19 completion stay unqualified until closure/freeze and matched evidence.'
body=body.replace(anchor,addition+'\n\n'+anchor,1)
start=body.index(anchor); end=body.index('\n\n',start)
coord='**Adjacent completion coordination:** the R1 owner reports its own ROOT accepted230 normal+230 race cases at exactGo1.26.3 source `190c30`/10064 paths, with actual join. Root has not adopted its full raw packet or proposed member-local ABI `49fe06`. Its C1-C5 and fail-closed C6 remain provisionally owned on its own paths; no ordinary wrapper import or external caching edits. Exact original completion/debt reconciliation remains required for #5111, without a blanket dependency or native activation.'
body=body[:start]+coord+body[end:]
body+='\n\nSelected-history checkpoint: '+now+'. Race component `'+component['sha256']+'`; coherent persistent-index decision `'+decision['sha256']+'`. Existing issue body and all native/M3/M8 gates preserved.\n'
assert api('repos/snissn/gomap/issues/comments/6054393776')['body']==comment['body']
api('repos/snissn/gomap/issues/comments/6054393776',{'body':body})
live=api('repos/snissn/gomap/issues/comments/6054393776')
assert live['body']==body and api('repos/snissn/gomap/issues/5111')['body']==issue_body
live_receipt=immutable('ordinary-selected-index-live-root-'+stamp+'.json',{'at_utc':now,'comment_url':live['html_url'],'comment_sha256':sha(body.encode()),'prior_comment':backup,'issue_body_preserved':True,'issue_body_sha256':sha(issue_body.encode()),'component':component,'decision':decision,'dependency_ready':False})

g=json.loads(local.read_bytes()); n=next(x for x in g['nodes'] if x['name']=='M7P')
others=json.dumps([x for x in g['nodes'] if x is not n],sort_keys=True)
c=dict(n['current_checkpoint']); c.update({'at_utc':now,'phase':'same-owner persistent-index history correction and actual completion/all19 closure before freeze','selected_rid_race_component':component,'persistent_index_decision':decision,'live_progress_receipt':live_receipt,'dependency_ready':False,'remaining_gate':'Complete canonical persistent history and original completion/debt/all19 production closure, affected normal/race/docs/CI impact, local product freeze then matched qualification and mature PR/root gates.'})
n['current_checkpoint']=c; n['dependency_ready']=False
g['m7_ordinary_extraction_current_checkpoint_20261008']=c
g['m7_ordinary_selected_history_frontier_20261008']={'component':component,'decision':decision,'live_receipt':live_receipt,'dependency_ready':False}
g['updated_utc']=now
assert others==json.dumps([x for x in g['nodes'] if x is not n],sort_keys=True)
new=(json.dumps(g,indent=2)+'\n').encode(); h=sha(new)
assert sha(local.read_bytes())==expected
code='from pathlib import Path\nimport sys,hashlib,json,os\np=Path('+repr(remote_graph)+')\nb=sys.stdin.read().encode()\nassert hashlib.sha256(p.read_bytes()).hexdigest()=='+repr(expected)+'\njson.loads(b)\nq=p.with_name('+repr('graph-state.selected-history-'+stamp+'.pending.json')+')\nwith q.open("xb") as f:f.write(b);f.flush();os.fsync(f.fileno())\nassert hashlib.sha256(q.read_bytes()).hexdigest()=='+repr(h)+'\nassert hashlib.sha256(p.read_bytes()).hexdigest()=='+repr(expected)+'\nos.replace(q,p)\nprint(json.dumps({"sha256":hashlib.sha256(p.read_bytes()).hexdigest()}))'
assert remote(code,new.decode())['sha256']==h
pending=local.with_name('graph-state.selected-history-'+stamp+'.pending.json')
with pending.open('xb') as f:f.write(new);f.flush();os.fsync(f.fileno())
assert sha(local.read_bytes())==expected
os.replace(pending,local)
assert sha(local.read_bytes())==h
receipt=immutable('ordinary-selected-index-graph-root-'+stamp+'.json',{'at_utc':now,'old_graph_sha256':expected,'new_graph_sha256':h,'external_remote_equal':True,'unrelated_nodes_preserved':True,'component':component,'decision':decision,'live_receipt':live_receipt,'dependency_ready':False,'released_paths':[]})
print(json.dumps({'component':component,'decision':decision,'receipt':receipt,'graph_sha256':h,'comment_url':live['html_url'],'dependency_ready':False}))
