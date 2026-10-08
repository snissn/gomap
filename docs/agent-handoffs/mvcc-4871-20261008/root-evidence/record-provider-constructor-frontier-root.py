from pathlib import Path
import ast, datetime, hashlib, json, os, shlex, subprocess

E = Path('/Volumes/FlashDrive/gomap-mvcc-4871-root-evidence-20261008')
H = Path('/Users/michaelseiler/dev/snissn/gomap/tmp/mvcc-m7-root-append-integration-20261003T1510Z/record-ordinary-metadata-budget-root-20261008.py')
O = H.parent / 'm7-ordinary-prerequisite-root-20261008'
tree = ast.parse(H.read_text())
keep = []
for node in tree.body:
    if isinstance(node, (ast.Import, ast.ImportFrom, ast.FunctionDef)):
        keep.append(node)
    elif isinstance(node, ast.Assign) and all(isinstance(t, ast.Name) and t.id in {'OUT', 'LOCAL', 'REMOTE', 'D', 'END'} for t in node.targets):
        keep.append(node)
ns = {}
exec(compile(ast.Module(body=keep, type_ignores=[]), str(H), 'exec'), ns)
ns['OUT'] = E
sha, api, remote, immutable = (ns[x] for x in ['sha', 'api', 'remote', 'immutable'])
G = E / 'graph-state.json'
expected = '46cba63a0c2da48b2ad5be619e8666847154928f01a19efce673d84aab906591'
assert sha(G.read_bytes()) == expected
assert remote('from pathlib import Path\nimport hashlib,json\nprint(json.dumps({"sha256":hashlib.sha256(Path('+repr(ns['REMOTE'])+').read_bytes()).hexdigest()}))')['sha256'] == expected
issue = api('repos/snissn/gomap/issues/5111')
assert issue['state'] == 'open' and issue['body'] == (O / 'ordinary-prerequisite-body.md').read_text()
now = datetime.datetime.now(datetime.timezone.utc)
stamp = now.strftime('%Y%m%dT%H%M%SZ')

code = r'''from pathlib import Path
import hashlib,json,re
b=Path('/home/mikers/benchmarks/mvcc-ordinary-5111-root-20261008');result=[]
for name in ['ordinary-owned-directory-record-scratch-valid-file-red','ordinary-owned-directory-record-flat-fixed']:
 p=b/name;r=json.loads((p/'result.json').read_bytes());before=(p/'source-before.json').read_bytes();after=(p/'source-after.json').read_bytes();raw=(p/'raw.log').read_bytes();text=raw.decode()
 assert before==after and hashlib.sha256(before).hexdigest()==r['before_sha256']==r['after_sha256'] and hashlib.sha256(raw).hexdigest()==r['raw_sha256']
 assert r['head']=='2d6b07f58902537cfe8d2b3e3a8c3c9ba0f3c07c'
 passes=re.findall(r'^--- PASS: (\S+)',text,re.M);failures=re.findall(r'^--- FAIL: (\S+)',text,re.M)
 if name.endswith('red'):assert r['exit']==1 and failures==['TestOwnedDirectoryRecordSerializationAdmissionAndExactOutput'] and 'serialization backing not admitted' in text
 else:assert r['exit']==0 and len(passes)==9 and '-race' in r['args'] and not failures and not re.search(r'(WARNING: DATA RACE|no tests to run|^--- SKIP:)',text,re.M)
 result.append({'directory':str(p),'result':r,'result_sha256':hashlib.sha256((p/'result.json').read_bytes()).hexdigest(),'source_maps_equal_verified':True,'actual_top_level_passes':passes,'actual_failures':failures,'raw_tail':text.splitlines()[-35:]})
print(json.dumps(result))'''
r = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', 'mikers@192.168.0.185', 'python3 -c ' + shlex.quote(code)], capture_output=True, text=True, check=True)
packets = json.loads(r.stdout)
component = immutable('ordinary-required-v2-admitted-serialization-component-root-'+stamp+'.json', {
    'at_utc':now.isoformat(), 'classification':'ACTUAL_UNADMITTED_SERIALIZATION_RED_AND_CONNECTED_RACE_GREEN_VERIFIED',
    'issue':issue['html_url'], 'packets':packets,
    'scope':'8 actual rootpublication and 1 actual DB top-level race cases, with both vacuum formats. Admission, exact output, empty/shared directory, exact RID and requirement scopes; no full19 or performance qualification.',
    'root_tests_rerun':False, 'dependency_ready':False, 'native_activation':False, 'released_paths':[]})
expanded = E / 'ordinary-original-outcome-expanded-race-component-root-20261008T154817Z.json'
assert sha(expanded.read_bytes()) == 'c53552a70b608a9a311f1e0637eabed9b89daed9f76b55d903524ce842a10a82'
decision = immutable('ordinary-legacy-provider-constructor-boundary-root-'+stamp+'.json', {
    'at_utc':now.isoformat(), 'issue':issue['html_url'], 'owner':'/root',
    'implementation_owner':'/root/m7_primary_directory_recovery_sol61', 'model':'gpt-6.1-sol',
    'classification':'CONTINUE_NARROW_ACTUAL_PROVIDER_CONSTRUCTOR_CLOSURE_WITHIN_ACCEPTED_BOUNDARY',
    'finding':'Actual dictdb/template captures build generic descriptors before selected import. Legacy child snapshots may lack PRIMARY, so the current owned pager constructor cannot use independently supplied parent metadata.',
    'root_inspected_source_bindings':{
        'TreeDB/db/stable_pager_owned_operations.go':'ae3fd2e694519f351f26878bc9fd1e6e201ce58c947c13cd09152017fea82738',
        'TreeDB/db/stable_resource.go':'1af56895d6c120a44e4dd8671d56f4434eb602ab8322c9f6b38010b8151eed04',
        'TreeDB/db/db.go':'398824168e1c115c0f0adae5ac2e6b6bc3b158770f3a4292bda11e5fe3742794',
        'TreeDB/internal/dictdb/resource_capture.go':'e48b995900ffacd0ae1a80ffbf10e1ad9675202b77169392502b5584a429ea3b',
        'TreeDB/internal/templatedb/resource_capture.go':'6f3b1772f49f651beccedefcdd218468b4fa09185910b64523c4b819b42b0e3f'},
    'decision':'Use explicit request-scoped supplied metadata Owner before new selected capture effects, separate from original child Snapshot/pager/physical registry custody. Reuse the SAME real final-reader finalizer and outcome with preborn failure custody at the original authority. Independently refund wrapper metadata only after actual success, retaining its parent governor through surviving allocation/debt. Preserve generic captures; do not force PRIMARY on the child, use global mutable capture context, relabel physical authority or treat opaque Close as completion.',
    'remaining_gate':'Actual inline/pointer producer capture, refusal rollback, original final-reader success/debt, full19 maturity/docs/CI impact/final base, coherent local product freeze, serial matched performance, mature PR and independent/current-head/root merge gates.',
    'original_outcome_component':{'local':str(expanded),'remote':ns['D']+'/'+expanded.name,'sha256':sha(expanded.read_bytes())},
    'required_v2_component':component, 'dependency_ready':False, 'new_issues':[], 'edge_changes':[], 'native_activation':False, 'released_paths':[]})

path = 'repos/snissn/gomap/issues/comments/6054393776'
comment = api(path)
assert '<!-- codex-issue-graph-executor:ordinary-focused-checkpoint -->' in comment['body']
assert '**Legacy child provider construction**' not in comment['body']
backup = immutable('ordinary-provider-frontier-comment-backup-root-'+stamp+'.json', comment)
addition = ('\n\n**Legacy child provider construction** (2026-10-08): Required-V2 serialization now has connected race evidence (8 rootpublication cases plus real vacuum in both formats; component '+component['sha256'][:8]+'). Original close/namespace/clone/refusal and actual leaf/column producers have 8 connected race cases (component c53552a7). These are component gates, not full19 maturity. The same sole worker is completing explicit admitted dictionary/template capture before construction, with separate parent metadata and original legacy child Snapshot/pager/registry custody through actual final-reader completion/debt. No forced child PRIMARY, opaque completion assumption, new owner/edge or universal generic quota. Then full19 affected checks/docs, coherent local product freeze, quiet serial matched qualification and PR gates. Native and M8 remain held.')
body = comment['body'] + addition
assert api(path)['body'] == comment['body']
api(path, {'body':body})
updated = api(path)
assert updated['body'] == body
assert api('repos/snissn/gomap/issues/5111')['body'] == issue['body']
live = immutable('ordinary-provider-frontier-live-root-'+stamp+'.json', {'at_utc':now.isoformat(), 'decision':decision, 'comment_url':updated['html_url'], 'comment_body_sha256':sha(body.encode()), 'issue_body_unchanged':True, 'backup':backup})

g = json.loads(G.read_bytes())
n = next(x for x in g['nodes'] if x['name']=='M7P')
others = json.dumps([x for x in g['nodes'] if x is not n], sort_keys=True)
c = dict(n['current_checkpoint'])
c.update({'at_utc':now.isoformat(), 'phase':'selected legacy dictionary/template provider construction and original final-reader completion before full19 maturity', 'dependency_ready':False, 'legacy_provider_constructor_boundary':decision, 'required_v2_serialization_component':component, 'original_outcome_expanded_component':decision['original_outcome_component'], 'next_gate':decision['remaining_gate']})
n.update({'current_checkpoint':c, 'dependency_ready':False, 'next_action':decision['remaining_gate']})
g['m7_ordinary_extraction_current_checkpoint_20261008']=c
g['updated_utc']=now.isoformat()
assert others==json.dumps([x for x in g['nodes'] if x is not n], sort_keys=True)
b=(json.dumps(g,indent=2)+'\n').encode();newsha=sha(b)
assert sha(G.read_bytes())==expected
code='from pathlib import Path\nimport sys,hashlib,json,os\np=Path('+repr(ns['REMOTE'])+')\nb=sys.stdin.read().encode()\nassert hashlib.sha256(p.read_bytes()).hexdigest()=='+repr(expected)+'\njson.loads(b)\nq=p.with_name("graph-state.provider-constructor.pending.json")\nwith q.open("xb") as f:f.write(b);f.flush();os.fsync(f.fileno())\nassert hashlib.sha256(q.read_bytes()).hexdigest()=='+repr(newsha)+'\nassert hashlib.sha256(p.read_bytes()).hexdigest()=='+repr(expected)+'\nos.replace(q,p)\nprint(json.dumps({"sha256":hashlib.sha256(p.read_bytes()).hexdigest()}))'
assert remote(code,b.decode())['sha256']==newsha
assert sha(G.read_bytes())==expected
pending=G.with_name('graph-state.provider-constructor.pending.json')
with pending.open('xb') as f:f.write(b);f.flush();os.fsync(f.fileno())
os.replace(pending,G)
assert sha(G.read_bytes())==newsha
receipt=immutable('ordinary-provider-frontier-graph-root-'+stamp+'.json', {'at_utc':now.isoformat(),'old_graph_sha256':expected,'new_graph_sha256':newsha,'local_remote_equal':True,'unrelated_nodes_preserved':True,'decision':decision,'component':component,'live':live,'dependency_ready':False})
print(json.dumps({'graph_sha256':newsha,'decision':decision,'component':component,'receipt':receipt},indent=2))
