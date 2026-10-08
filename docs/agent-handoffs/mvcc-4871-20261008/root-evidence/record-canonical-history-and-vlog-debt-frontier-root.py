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
local, remote_graph = external / 'graph-state.json', ns['REMOTE']
expected = '9c1209c2c7caaff7079a6e5610f7cd1f98890c91806d7e4e1c75c73cd5c95a81'
assert sha(local.read_bytes()) == expected
assert remote('from pathlib import Path\nimport hashlib,json\nprint(json.dumps({"sha256":hashlib.sha256(Path('+repr(remote_graph)+').read_bytes()).hexdigest()}))')['sha256'] == expected
now = datetime.datetime.now(datetime.timezone.utc).isoformat()
stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')

code = r'''
import pathlib,json,hashlib,re
b=pathlib.Path('/home/mikers/benchmarks/mvcc-ordinary-5111-root-20261008')
h=lambda x:hashlib.sha256(x).hexdigest()
specs=[('ordinary-owned-canonical-history-full','1ef5ea753ac9c74cf531ebce8027673090f4b4b3805220a22b3a80a0a94ee2e9','51336d490881a40bd9d1cb480566422c0eefb82d0e04ea0914b52c51db37b587',0,7,0,True),('ordinary-owned-history-and-vlog-debt-red','aa2c08f57b1761d86391d058ccacb56c1c8105993d5abe7f63c39ccb51774996','dd392d46a3c7a8108092f3c5d19290b78bee0c730b5ec80b3feb85f2509f7d97',1,0,2,False),('ordinary-owned-history-and-vlog-debt-green','c2fe83544a861fc46ac63dabac040672bfc282f48c3cfe80f110487158e05e0e','bcbaa98944e302a51bd0fad3e838e00ac09103ea951f979b4fb5d4c87ff29e96',1,10,0,True)]
keys=['TreeDB/db/db.go','TreeDB/db/stable_pager_owned_operations.go','TreeDB/db/stable_pager_owned_operations_test.go','TreeDB/internal/rootpublication/resource_coalesce_allocation.go','TreeDB/internal/rootpublication/resource_coalesce_allocation_test.go','TreeDB/internal/rootpublication/resource_entry_outgoing_allocation.go','TreeDB/internal/rootpublication/resource_import_allocation.go','TreeDB/internal/rootpublication/resource_requirements_allocation.go','TreeDB/internal/rootpublication/resource_manifest_allocation.go']
out=[]
for name,rawsha,mapsha,exitcode,passcount,failcount,race in specs:
 p=b/name; raw=(p/'raw.log').read_bytes(); before=(p/'source-before.json').read_bytes(); after=(p/'source-after.json').read_bytes(); m=json.loads(before); r=json.loads((p/'result.json').read_bytes());t=raw.decode()
 assert h(raw)==r['raw_sha256']==rawsha and before==after and h(before)==r['before_sha256']==r['after_sha256']==mapsha
 assert r['source_equal'] and r['exit']==exitcode and len(m)==r['source_count']==10031
 assert r['source']==str(b/'source') and r['head']=='2d6b07f58902537cfe8d2b3e3a8c3c9ba0f3c07c'
 assert r['env']['GOWORK']=='off' and r['env']['GOTOOLCHAIN']=='local' and r['env']['GOROOT'].endswith('go1.26.4.linux-amd64')
 assert ('-race' in r['args'])==race and '-count=1' in r['args']
 passes=re.findall(r'^--- PASS: (\S+)',t,re.M);fails=re.findall(r'^--- FAIL: (\S+)',t,re.M);skips=re.findall(r'^--- SKIP: (\S+)',t,re.M)
 assert len(passes)==passcount and len(fails)==failcount and not skips
 if name.endswith('-red'):assert 'temporary-only governor refunded surviving original cleanup debt' in t
 if name.endswith('-green'):assert 'goleak: Errors on successful test run' in t and '(*Pager).growLoop' in t
 out.append({'directory':str(p),'result':r,'result_sha256':h((p/'result.json').read_bytes()),'actual_top_level_passes':passes,'actual_top_level_failures':fails,'actual_top_level_skips':skips,'source_bindings':{k:m[k] for k in keys if k in m},'current_binding_matches':{k:h((b/'source'/k).read_bytes())==m[k] for k in keys if k in m},'process_accepted':exitcode==0,'raw_tail':t.splitlines()[-15:] if exitcode else []})
print(json.dumps(out))
'''
packets=json.loads(subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','mikers@192.168.0.185','python3 -c '+shlex.quote(code)],capture_output=True,text=True,check=True).stdout)
receipt=immutable('ordinary-canonical-history-and-vlog-debt-frontier-root-'+stamp+'.json',{
    'at_utc':now,'issue':'https://github.com/snissn/gomap/issues/5111','classification':'CANONICAL_HISTORY_RACE_COMPONENT_ACCEPTED_REAL_DEBT_RED_CONFIRMED_REPAIR_ASSERTIONS_PASS_PROCESS_GATE_RED',
    'packets':packets,'root_tests_rerun':False,'dependency_ready':False,'performance_qualified':False,
    'source_cut_reference':{'local':str(external/'ordinary-history-and-debt-source-cut-root-20261008T145148Z.json'),'sha256':'86897cb41d72c2dbc88974823aa8dd9caaf8ffe492a4b36b3523fb6b43fc62c9'},
    'finding':'Actual Snapshot finalizer consumes original VLog Set/File references and unlinks the real zombie. A deferred parent-sync failure survives, but PRIMARY-only CloseAfterCleanup previously detached the temporary-only governor before pager completion retained that debt.',
    'repair':'The same finalizer now joins all original cleanup outcomes into CloseAfterCleanup(err) before enrollment release. Race assertions verify exact failure custody, positive budget charge and no replay of consumed original references. Package exit still fails goleak because the fixture left Pager.growLoop alive; this packet is not an accepted repair gate.',
    'scope':'Same existing all19 completion packet, original authorities and no new ownership/API/dependency. Canonical persistent history7-race component is accepted at its original exact source; changed source must carry applicability explicitly.',
    'next_action':'Same sole worker fixes fixture teardown without weakening real debt/no-replay assertions, then coherent all19 closure and affected risk checks/docs/CI impact before product freeze and matched performance.',
    'new_issues':[],'new_owners':[],'edge_changes':[],'released_paths':[],'native_activation':False})

issue_body=(helper.parent/'m7-ordinary-prerequisite-root-20261008/ordinary-prerequisite-body.md').read_text()
issue=api('repos/snissn/gomap/issues/5111')
assert issue['state']=='open' and issue['body']==issue_body
comment=api('repos/snissn/gomap/issues/comments/6054393776');body=comment['body']
assert body.startswith('<!-- codex-issue-graph-executor:ordinary-focused-checkpoint -->') and '**Canonical history and actual VLog debt:**' not in body
backup=ns['immutable_bytes']('ordinary-focused-comment-before-vlog-debt-root-'+stamp+'.md',body.encode())
anchor='**Adjacent completion coordination:**';assert anchor in body
addition='**Canonical history and actual VLog debt:** root verified7 actual race passes (`1ef5ea75`, equal10031-path map `51336d49`) covering canonical complete history, same-identity custody, imports and DB publication. A real post-unlink parent-sync failure then reproduced lost budget governance (`aa2c08f5`, map `dd392d46`): original VLog Set/File references were consumed but cleanup debt survived. The finalizer now preserves the combined original cleanup outcome before enrollment release. Ten repair/history race assertions pass, including debt charge and no replay, but that packet (`c2fe8354`, map `bcbaa989`) exits1 due to a leaked fixture Pager grower; it is not an accepted green gate. The same sole Sol6.1 worker owns fixture teardown and all19 closure before freeze, matched evidence and PR maturity. No new issue, authority, edge, native activation or gate relaxation.'
body=body.replace(anchor,addition+'\n\n'+anchor,1)
body+='\n\nCanonical-history/debt checkpoint: '+now+'. Root receipt `'+receipt['sha256']+'`. Existing issue body and native/M3/M8 gates preserved.\n'
assert api('repos/snissn/gomap/issues/comments/6054393776')['body']==comment['body']
api('repos/snissn/gomap/issues/comments/6054393776',{'body':body})
live=api('repos/snissn/gomap/issues/comments/6054393776')
assert live['body']==body and api('repos/snissn/gomap/issues/5111')['body']==issue_body
live_receipt=immutable('ordinary-canonical-history-and-debt-live-root-'+stamp+'.json',{'at_utc':now,'comment_url':live['html_url'],'comment_sha256':sha(body.encode()),'prior_comment':backup,'issue_body_preserved':True,'issue_body_sha256':sha(issue_body.encode()),'component':receipt,'dependency_ready':False})

g=json.loads(local.read_bytes());n=next(x for x in g['nodes'] if x['name']=='M7P')
others=json.dumps([x for x in g['nodes'] if x is not n],sort_keys=True)
c=dict(n['current_checkpoint']);c.update({'at_utc':now,'phase':'canonical persistent history component and actual VLog debt repair; fixture process gate then all19 closure before freeze','canonical_history_and_vlog_debt_frontier':receipt,'live_progress_receipt':live_receipt,'dependency_ready':False,'remaining_gate':'Fix actual fixture teardown without weakening debt/no-replay, complete all19 production closure and affected normal/race/docs/CI impact, local product freeze then matched qualification, mature PR and root review/current-head CI/merge.'})
n['current_checkpoint']=c;n['dependency_ready']=False
g['m7_ordinary_extraction_current_checkpoint_20261008']=c
g['m7_ordinary_canonical_history_and_vlog_debt_frontier_20261008']={'component':receipt,'live_receipt':live_receipt,'dependency_ready':False}
g['updated_utc']=now
assert others==json.dumps([x for x in g['nodes'] if x is not n],sort_keys=True)
new=(json.dumps(g,indent=2)+'\n').encode();h=sha(new)
assert sha(local.read_bytes())==expected
code='from pathlib import Path\nimport sys,hashlib,json,os\np=Path('+repr(remote_graph)+')\nb=sys.stdin.read().encode()\nassert hashlib.sha256(p.read_bytes()).hexdigest()=='+repr(expected)+'\njson.loads(b)\nq=p.with_name('+repr('graph-state.history-debt-'+stamp+'.pending.json')+')\nwith q.open("xb") as f:f.write(b);f.flush();os.fsync(f.fileno())\nassert hashlib.sha256(q.read_bytes()).hexdigest()=='+repr(h)+'\nassert hashlib.sha256(p.read_bytes()).hexdigest()=='+repr(expected)+'\nos.replace(q,p)\nprint(json.dumps({"sha256":hashlib.sha256(p.read_bytes()).hexdigest()}))'
assert remote(code,new.decode())['sha256']==h
pending=local.with_name('graph-state.history-debt-'+stamp+'.pending.json')
with pending.open('xb') as f:f.write(new);f.flush();os.fsync(f.fileno())
assert sha(local.read_bytes())==expected
os.replace(pending,local);assert sha(local.read_bytes())==h
graph_receipt=immutable('ordinary-canonical-history-and-debt-graph-root-'+stamp+'.json',{'at_utc':now,'old_graph_sha256':expected,'new_graph_sha256':h,'external_remote_equal':True,'unrelated_nodes_preserved':True,'component':receipt,'live_receipt':live_receipt,'dependency_ready':False,'released_paths':[]})
print(json.dumps({'receipt':receipt,'graph_receipt':graph_receipt,'graph_sha256':h,'comment_url':live['html_url'],'dependency_ready':False}))
