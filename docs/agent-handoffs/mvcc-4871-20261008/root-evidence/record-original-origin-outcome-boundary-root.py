from pathlib import Path
import ast, datetime, hashlib, json, os

helper = Path('/Users/michaelseiler/dev/snissn/gomap/tmp/mvcc-m7-root-append-integration-20261003T1510Z/record-ordinary-metadata-budget-root-20261008.py')
tree = ast.parse(helper.read_text())
allowed = {'OUT', 'LOCAL', 'REMOTE', 'D', 'END'}
parts = [n for n in tree.body if isinstance(n, (ast.Import, ast.ImportFrom, ast.FunctionDef)) or (isinstance(n, ast.Assign) and all(isinstance(t, ast.Name) and t.id in allowed for t in n.targets))]
ns = {}
exec(compile(ast.Module(body=parts, type_ignores=[]), str(helper), 'exec'), ns)
external = Path('/Volumes/FlashDrive/gomap-mvcc-4871-root-evidence-20261008')
ns['OUT'] = external
api, remote, immutable, sha = [ns[k] for k in ('api', 'remote', 'immutable', 'sha')]
local, remote_graph = external / 'graph-state.json', ns['REMOTE']
expected = '2d3a61eb01091e998b0dbaf0e712aebe1ee63a5dbcc67f655df62e3126194e08'
assert sha(local.read_bytes()) == expected
assert remote('from pathlib import Path\nimport hashlib,json\nprint(json.dumps({"sha256":hashlib.sha256(Path('+repr(remote_graph)+').read_bytes()).hexdigest()}))')['sha256'] == expected
now = datetime.datetime.now(datetime.timezone.utc).isoformat()
stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
bindings = {
 'TreeDB/internal/rootpublication/resource_token.go': 'f4eed8800595418a99d93c860d37d1f27a6bad0b923a0743923745493fe49b87',
 'TreeDB/internal/rootpublication/resource_token_allocation.go': 'a560c17011dad6d9ab7972d93bf8fb531d562711178d915319bde82672ec984e',
 'TreeDB/internal/rootpublication/resource_import_allocation.go': '3923a45f905479263870a91faf531ab415c889b07996335be4e68eac7641cd69',
 'TreeDB/internal/rootpublication/resource_entry_view.go': '8494546aef68bcba352e9cada90e20b77594246e91e46aba6c0a0853e1fe2f15',
 'TreeDB/internal/rootpublication/pin_registry.go': '88827a89ae60231bc00c702a1d61c208eeae9d55ed0b9d08e4dd2de50eda5998',
 'TreeDB/db/durable_root_runtime.go': 'b14dda96b24ec69020c447d936b9d56b3eb485b91f17b57cea038610c0a09ffc',
 'TreeDB/db/leaf_generation_manifest_store.go': 'ed4b42d3c65740bd7272a00085d42c020c576ef341f89199ce6b8b32a3e8fff7',
 'TreeDB/collections/stable_resource.go': 'a2630ccc0cfe07b1058a489dc5d39fe5d922b6ff9ac2836f4885e5d46a69de6a',
}
decision = immutable('ordinary-original-origin-release-outcome-boundary-root-'+stamp+'.json', {
 'at_utc': now, 'issue': 'https://github.com/snissn/gomap/issues/5111',
 'classification': 'NARROW_ORIGINAL_PRODUCTION_ORIGIN_RELEASE_OUTCOME_REPAIR_ACCEPTED_WITHIN_EXISTING_ALL19',
 'source': '192.168.0.185:/home/mikers/benchmarks/mvcc-ordinary-5111-root-20261008/source',
 'base': '2d6b07f58902537cfe8d2b3e3a8c3c9ba0f3c07c', 'inspected_source_bindings': bindings,
 'finding': 'Imported descriptor retains the genuine source-kind rope. Its final release invokes the original void cleanup; generic original token and namespace paths discard File.Close errors and known original Unobserve-only callbacks discard the registry outcome. Adapter disposal therefore does not prove original physical completion.',
 'original_origins': ['durable-root recovered dependency', 'leaf-manifest revision', 'stable column/template/vector asset'],
 'accepted_boundary': 'Preserve original physical/namespace/registry producer authority and one original action. Add only the typed/internal consumed/deferred/completed/retained-debt outcome at those actual adopted origins; preserve legacy public void Release compatibility. Replace their known Unobserve-only callback with an exact original typed observation edge when it simplifies the same ownership boundary. Do not copy arbitrary opaque callbacks or assume they succeeded.',
 'custody': 'Failed last-source close/namespace/Unobserve remains truthful original registry/producer debt with surviving backing; consumed action must not replay. Independent selected adapter backing refunds only at actual disposal. Preadmit new outcome/debt backing before effects, preferring existing original intrusive custody.',
 'acceptance': 'Actual producer/import/last-source/held-alias, failure/refusal and once-release checks; coherent all19 normal/race closure, docs/CI impact then local product freeze and matched allocation/retention/storage/performance, mature PR/root review/current-head CI/final-base/merge.',
 'not_accepted': ['implementation not yet verified', 'foreign R1 proposal49fe06 not adopted', 'no universal generic provider quota or speculative sweep', 'no second publisher/budget/registry', 'no native eligibility or gate relaxation'],
 'implementation_owner': '/root/m7_primary_directory_recovery_sol61', 'model': 'gpt-6.1-sol',
 'new_issues': [], 'new_owners': [], 'edge_changes': [], 'released_paths': [], 'dependency_ready': False,
})
issue_body = (helper.parent/'m7-ordinary-prerequisite-root-20261008/ordinary-prerequisite-body.md').read_text()
assert api('repos/snissn/gomap/issues/5111')['body'] == issue_body
comment = api('repos/snissn/gomap/issues/comments/6054393776')
old_body = comment['body']
assert old_body.startswith('<!-- codex-issue-graph-executor:ordinary-focused-checkpoint -->')
anchor = '**Adjacent completion coordination:**'
assert anchor in old_body and '**Original production release outcomes:**' not in old_body
body = old_body.replace(anchor, '**Original production release outcomes:** root inspected the genuine imported source-kind custody and the three actual recovered-dependency, leaf-manifest and collection-asset origins. Generic original file/namespace cleanup and known Unobserve-only callbacks currently discard errors; successful adapter disposal cannot certify physical completion. The same sole Sol6.1 worker will repair only that necessary original typed/internal outcome boundary, preserving the same original registry/producer, consumed versus deferred/completed/debt state, truthful backing and once-only action. Legacy public void release remains compatible; no arbitrary callback copying, universal provider quota, second authority, R1 proposal adoption or new dependency. This is an accepted repair direction, not a verified implementation or maturity gate.\n\n'+anchor, 1)
body += '\n\nOriginal-origin outcome decision: '+now+'. Root receipt `'+decision['sha256']+'`; all issue-body and native/M3/M8 gates preserved.\n'
backup = ns['immutable_bytes']('ordinary-focused-comment-before-original-origin-root-'+stamp+'.md', old_body.encode())
assert api('repos/snissn/gomap/issues/comments/6054393776')['body'] == old_body
api('repos/snissn/gomap/issues/comments/6054393776', {'body': body})
live = api('repos/snissn/gomap/issues/comments/6054393776')
assert live['body'] == body and api('repos/snissn/gomap/issues/5111')['body'] == issue_body
live_receipt = immutable('ordinary-original-origin-release-live-root-'+stamp+'.json', {'at_utc': now, 'comment_url': live['html_url'], 'comment_sha256': sha(body.encode()), 'prior_comment': backup, 'issue_body_preserved': True, 'issue_body_sha256': sha(issue_body.encode()), 'decision': decision})
g = json.loads(local.read_bytes())
n = next(x for x in g['nodes'] if x['name'] == 'M7P')
others = json.dumps([x for x in g['nodes'] if x is not n], sort_keys=True)
c = dict(n['current_checkpoint'])
c.update({'at_utc': now, 'phase': 'selected actual production telemetry validation and original-origin cleanup outcome closure before maturity', 'original_origin_release_outcome_decision': decision, 'live_progress_receipt': live_receipt, 'dependency_ready': False, 'remaining_gate': 'Same sole worker completes the accepted three original-origin outcome repair and actual selected telemetry/provider/import/all19 closure, affected normal/race/docs/CI impact; local product freeze then matched qualification, mature PR/root exact-head review/CI/final-base/merge.'})
n['current_checkpoint'] = c
n['dependency_ready'] = False
g['m7_ordinary_extraction_current_checkpoint_20261008'] = c
g['m7_ordinary_original_origin_release_outcome_decision_20261008'] = {'decision': decision, 'live_receipt': live_receipt, 'dependency_ready': False}
g['updated_utc'] = now
assert others == json.dumps([x for x in g['nodes'] if x is not n], sort_keys=True)
new = (json.dumps(g, indent=2)+'\n').encode()
h = sha(new)
assert sha(local.read_bytes()) == expected
code = 'from pathlib import Path\nimport sys,hashlib,json,os\np=Path('+repr(remote_graph)+')\nb=sys.stdin.read().encode()\nassert hashlib.sha256(p.read_bytes()).hexdigest()=='+repr(expected)+'\njson.loads(b)\nq=p.with_name('+repr('graph-state.original-outcome-'+stamp+'.pending.json')+')\nwith q.open("xb") as f:f.write(b);f.flush();os.fsync(f.fileno())\nassert hashlib.sha256(q.read_bytes()).hexdigest()=='+repr(h)+'\nassert hashlib.sha256(p.read_bytes()).hexdigest()=='+repr(expected)+'\nos.replace(q,p)\nprint(json.dumps({"sha256":hashlib.sha256(p.read_bytes()).hexdigest()}))'
assert remote(code, new.decode())['sha256'] == h
pending = local.with_name('graph-state.original-outcome-'+stamp+'.pending.json')
with pending.open('xb') as f:
 f.write(new); f.flush(); os.fsync(f.fileno())
assert sha(local.read_bytes()) == expected
os.replace(pending, local)
assert sha(local.read_bytes()) == h
graph_receipt = immutable('ordinary-original-origin-release-graph-root-'+stamp+'.json', {'at_utc': now, 'old_graph_sha256': expected, 'new_graph_sha256': h, 'external_remote_equal': True, 'unrelated_nodes_preserved': True, 'decision': decision, 'live_receipt': live_receipt, 'dependency_ready': False, 'released_paths': []})
print(json.dumps({'decision': decision, 'graph_receipt': graph_receipt, 'graph_sha256': h, 'comment_url': live['html_url']}))
