from pathlib import Path
import ast, datetime, hashlib, json, os

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
primary, remote_graph, decision_dir = [ns[k] for k in ('LOCAL', 'REMOTE', 'D')]
expected = '37dd07e33c2dba26fc4f17aadc76525973d2007bfe1dea6f7b8da1cda3ef9a5b'
assert sha(primary.read_bytes()) == expected
assert remote('from pathlib import Path\nimport hashlib,json\nprint(json.dumps({"sha256":hashlib.sha256(Path('+repr(remote_graph)+').read_bytes()).hexdigest()}))')['sha256'] == expected
now = datetime.datetime.now(datetime.timezone.utc).isoformat()
stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
component_name = 'ordinary-import-and-requirements-components-root-20261008T140857Z.json'
component_sha = '84e199381e6d8f53870c49bad49a36dc60b8f36c5e0e8a2007a9a852028f0df5'
assert sha((external/component_name).read_bytes()) == component_sha
component = {'local':str(external/component_name), 'remote':decision_dir+'/'+component_name, 'sha256':component_sha}
issue = api('repos/snissn/gomap/issues/5111')
assert issue['state'] == 'open'
old_body = (helper.parent/'m7-ordinary-prerequisite-root-20261008/ordinary-prerequisite-body.md').read_text()
assert issue['body'] == old_body
comment = api('repos/snissn/gomap/issues/comments/6054393776')
body = comment['body']
assert body.startswith('<!-- codex-issue-graph-executor:ordinary-focused-checkpoint -->')
backup = ns['immutable_bytes']('ordinary-focused-comment-before-import-storage-root-'+stamp+'.md', body.encode())

def replace_paragraph(value, heading, text):
    start = value.index(heading)
    end = value.index('\n\n', start)
    return value[:start]+text+value[end:]

body = replace_paragraph(body, '**Selected import helper verified:**',
    '**Selected import construction verified:** root checked six actual race cases (`2bbbcd0f`, equal10028-path map `e315b35f`): original-operation custody, actual identity collisions/exact RID union, partial capacity refusal, shared directories and empty-directory leases. The selected path now creates one preadmitted flattened canonical directory per exact root and reuses it for sizing/copy passes. Seven race cases then pass at the extended requirements source (`92c56e0d`, equal10029-path map `3ced4886`), covering the focused global/namespace parity and missing/conflict refusal cases. Root did not rerun them. The unscoped-nonempty requirements error-class parity edge has a retained RED and is being repaired by the same worker. Void foreign cohort release proves adapter detachment only; actual physical/namespace/debt outcomes remain with their original owner. These are construction components, with no elapsed-time, production-routing or whole-candidate qualification.')
body = replace_paragraph(body, '**Host/cleanup:**',
    '**Host/cleanup:** the sole ordinary source/focused Go owner continues on .185. Native4878 source/evidence on .111 remains frozen; separately owned functional work has its own180GiB free-space floor/12GiB attributable cap and joined receipts. Prospective S5098 discovery coordination retains180GiB floor/12GiB S cap/24GiB total R cap; it grants no runtime, source adoption, native activation or .185 work. The previously released local Go cache is verified absent with observed195,317,760B recovery. A fresh bounded local cleanup audit found no surviving released target and no eligible new cache; zero additional deletion/reclaim. Local Mac writes now fail ENOSPC even at2KiB. Protected source/raw/maps/negative packets remain. New root receipts and the current graph mirror use `/Volumes/FlashDrive/gomap-mvcc-4871-root-evidence-20261008`, verified mounted with39,406,809,088B free before use; remote source/evidence remains usable. This is new-evidence placement, not cleanup or migration of retained worktrees.')
body += '\n\nImport/storage checkpoint: '+now+'. Component receipt `'+component_name+'` (`'+component_sha+'`), exact FlashDrive/remote bytes verified. Production selected import/new-provider capture and exact completion/debt closure remain the sole critical path. All19 groups, local product freeze, matched performance, mature PR/review/latest CI/final-base/merge remain open. No dependency-ready, cap/edge change, native activation or M8 unblock.\n'
assert api('repos/snissn/gomap/issues/comments/6054393776')['body'] == comment['body']
api('repos/snissn/gomap/issues/comments/6054393776', {'body':body})
live = api('repos/snissn/gomap/issues/comments/6054393776')
assert live['body'] == body
assert api('repos/snissn/gomap/issues/5111')['body'] == old_body
live_receipt = immutable('ordinary-import-storage-live-root-'+stamp+'.json', {
    'at_utc':now,'comment_url':live['html_url'],'body_sha256':sha(body.encode()),
    'prior_comment':backup,'issue_body_preserved':True,'issue_body_sha256':sha(old_body.encode()),
    'component':component,'dependency_ready':False})

g = json.loads(primary.read_bytes())
n = next(x for x in g['nodes'] if x['name'] == 'M7P')
others = json.dumps([x for x in g['nodes'] if x is not n], sort_keys=True)
c = dict(n['current_checkpoint'])
c.update({'at_utc':now,'phase':'production pre-first-visible-Clone and new-provider admission; exact completion/debt closure',
          'import_construction_and_requirements_components':component,'live_progress_receipt':live_receipt,
          'dependency_ready':False,'tests_rerun_by_root':False,
          'remaining_gate':'Production selected import/new captures, exact original completion/debt and all19 closure, affected validation, local product freeze, matched qualification and mature PR/root gates.'})
fallback = {'at_utc':now,'cause':'Local Mac ENOSPC prevents even2KiB writes',
            'remote_authority':remote_graph,'current_local_mirror':str(external/'graph-state.json'),
            'protected_prior_primary_mirror':str(primary),'prior_primary_sha256':expected,
            'new_receipt_root':str(external),'mount_verified':True,'preflight_free_bytes':39406809088,
            'latest_cleanup':'No new eligible deletion; prior exact released targets absent; protected negative/source/evidence retained.',
            'source_execution_host':'192.168.0.185','source_work_continues':True,
            'cleanup_completed_new_deletion':False}
c['storage_fallback'] = fallback
n['current_checkpoint'] = c
n['dependency_ready'] = False
n['storage_fallback'] = fallback
g['m7_ordinary_extraction_current_checkpoint_20261008'] = c
g['ordinary_storage_fallback_20261008'] = fallback
g['updated_utc'] = now
assert others == json.dumps([x for x in g['nodes'] if x is not n], sort_keys=True)
new = (json.dumps(g, indent=2)+'\n').encode()
h = sha(new)
assert sha(primary.read_bytes()) == expected
code = 'from pathlib import Path\nimport sys,hashlib,json,os\np=Path('+repr(remote_graph)+')\nb=sys.stdin.read().encode()\nassert hashlib.sha256(p.read_bytes()).hexdigest()=='+repr(expected)+'\njson.loads(b)\nq=p.with_name('+repr('graph-state.import-storage-'+stamp+'.pending.json')+')\nwith q.open("xb") as f:f.write(b);f.flush();os.fsync(f.fileno())\nassert hashlib.sha256(q.read_bytes()).hexdigest()=='+repr(h)+'\nassert hashlib.sha256(p.read_bytes()).hexdigest()=='+repr(expected)+'\nos.replace(q,p)\nprint(json.dumps({"sha256":hashlib.sha256(p.read_bytes()).hexdigest()}))'
assert remote(code, new.decode())['sha256'] == h
local = external/'graph-state.json'
with local.open('xb') as f:
    f.write(new);f.flush();os.fsync(f.fileno())
assert sha(local.read_bytes()) == h
assert sha(primary.read_bytes()) == expected
receipt = immutable('ordinary-import-storage-graph-root-'+stamp+'.json', {
    'at_utc':now,'old_graph_sha256':expected,'new_graph_sha256':h,
    'current_external_remote_equal':True,'primary_preserved_at_old_sha256':True,
    'unrelated_nodes_preserved':True,'component':component,'live_receipt':live_receipt,
    'storage_fallback':fallback,'dependency_ready':False})
print(json.dumps({'receipt':receipt,'graph_sha256':h,'local_current_mirror':str(local),
                  'comment_url':live['html_url'],'dependency_ready':False}))
