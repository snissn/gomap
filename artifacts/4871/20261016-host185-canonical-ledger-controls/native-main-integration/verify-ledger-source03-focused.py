from pathlib import Path
import hashlib,json,re,datetime
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');B=D/'native-main-integration/native-ledger-source03';O=B/'controls'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
read=lambda p:json.loads(p.read_text())
s=read(O/'summary.json');m=read(B/'source-frozen.json');p=read(B/'proposed-commands.json')
commands=[c for c in p['commands'] if c['selection_class']=='focused_requires_root_runtime_grant']
assert s['commands_executed']==5 and not s['commands_not_executed'] and s['all_authorized_pass'] and s['all_groups_empty'] and s['source_still_frozen'] and s['git_identity_still_frozen']
assert [c['name'] for c in commands]==[r['name'] for r in s['results']]
checks=[]
def verify(c,P,manifest):
 r=read(P/'result.json');log=(P/'run.log').read_text();runs=re.findall(r'^=== RUN\s+(\S+)',log,re.M);passes=re.findall(r'^\s*--- PASS: (\S+)',log,re.M)
 expected=c['required_top_level_tests']+[parent+'/'+child for parent,children in c['required_child_tests'].items() for child in children]
 assert sorted(runs)==sorted(passes)==sorted(expected)
 assert r['status']=='PASS' and r['exit_code']==0 and not r['test_FAIL_names'] and not r['test_SKIP_names'] and not r['stop_reasons'] and not r['timeout']
 assert r['raw_log_sha256']==sha(P/'run.log') and runs==r['test_RUN_names'] and passes==r['test_PASS_names']
 assert read(P/'source-before.json')==read(P/'source-after.json')==manifest
 assert r['group_and_children_empty'] and not r['signals'] and not r['remaining_group_members'] and not r['remaining_direct_or_adopted_children'] and not r['residual_observed_before_cleanup']
 assert r['top_level_pass_count']==c['expected_top_level_count'] and r['child_RUN_count']==c['expected_child_count']
 return {'name':c['name'],'result':str(P/'result.json'),'result_sha256':sha(P/'result.json'),'raw_sha256':sha(P/'run.log'),'top_pass':r['top_level_pass_count'],'child_pass':r['child_RUN_count'],'groups_empty':True,'source_equal':True}
for c in commands:checks.append(verify(c,O/c['name'],m))
reusepath=D/'native-decision/root-native-ledger-source03-focused-reuse.json';reuse=read(reusepath)
assert reuse['source_manifest_sha256']==sha(B/'source-frozen.json') and reuse['accepted']
retained=[]
for r in reuse['results']:
 packet=Path(r['packet']);proposal=read(packet/'proposed-commands.json');c=next(c for c in proposal['commands'] if c['name']==r['name'])
 verified=verify(c,packet/'controls'/r['name'],read(packet/'source-frozen.json'))
 assert verified['result_sha256']==r['result_sha256'] and verified['raw_sha256']==r['raw_sha256']
 retained.append(verified)
fresh={mode:{'top':sum(r['top_pass'] for r in checks if r['name'].startswith(mode+'-')),'child':sum(r['child_pass'] for r in checks if r['name'].startswith(mode+'-'))} for mode in ('normal','race')}
assert fresh=={'normal':{'top':18,'child':10},'race':{'top':101,'child':178}}
assert sum(r['top_pass'] for r in retained)==83 and sum(r['child_pass'] for r in retained)==168
x={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'Complete ledger focused normal/race controls with exact-input historical normal reuse','accepted':True,'source_manifest_sha256':sha(B/'source-frozen.json'),'summary_sha256':sha(O/'summary.json'),'checks':checks,'fresh':fresh,'reuse_receipt_sha256':sha(reusepath),'reused':retained,'effective_per_mode':{'top':101,'child':178},'full_source_and_index_unchanged':True,'all_groups_empty':True,'source01_and02_failures_preserved':True,'actual_publication_accepted':False,'Q32':'original RED/ACK0','graph_qualification':'OPEN'}
out=D/'native-decision/root-native-ledger-source03-focused-verification.json'
with out.open('x') as f:json.dump(x,f,indent=2);f.write('\n')
print(json.dumps({'receipt':str(out),'sha256':sha(out),'fresh':fresh,'effective_per_mode':x['effective_per_mode']}))
