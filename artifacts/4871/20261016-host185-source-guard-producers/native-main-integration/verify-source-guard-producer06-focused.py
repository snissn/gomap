import datetime, hashlib, json, os, re
from pathlib import Path
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
B=D/'native-main-integration/native-source-guard-producer06'
W=D/'native-integrated'
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p): return json.loads(p.read_text())
manifest=read(B/'source-frozen.json');summary=read(B/'controls/summary.json')
assert summary['all_authorized_pass'] and summary['all_groups_empty'] and summary['source_still_frozen'] and summary['git_identity_still_frozen']
selection=read(D/'native-decision/root-native-source-guard-producer06-focused-selection.json')
rows=[]
for record in summary['results']+selection['retained_prior_results']:
 p=Path(record['result']);r=read(p);out=p.parent;raw=(out/'run.log').read_text()
 assert r['status']=='PASS' and r['exit_code']==0 and r['leader_joined'] and r['group_and_children_empty']
 assert not r['signals'] and not r['remaining_group_members'] and not r['remaining_direct_or_adopted_children']
 assert sha(out/'run.log')==r['raw_log_sha256']
 assert read(out/'source-before.json')==read(out/'source-after.json')
 original=read(out/'source-before.json')
 assert original.keys()==manifest.keys()
 differences={n for n in manifest if original[n]!=manifest[n]}
 assert all(n.endswith('_test.go') for n in differences),differences
 package=r['name'].split('-',1)[1]
 relevant={'retainedalloc':'TreeDB/internal/retainedalloc/','lifecycle':'TreeDB/lifecycle/','memtable':'TreeDB/internal/memtable/','primaryarena':'TreeDB/internal/primaryarena/','rootpublication':'TreeDB/internal/rootpublication/','db':'TreeDB/db/','mvcc':'TreeDB/mvcc/'}[package]
 assert not any(n.startswith(relevant) for n in differences)
 runs=re.findall(r'^=== RUN\s+(\S+)',raw,re.M);passes=re.findall(r'^\s*--- PASS: (\S+)',raw,re.M)
 assert runs==r['test_RUN_names'] and passes==r['test_PASS_names'] and sorted(runs)==sorted(passes)
 assert not r['test_FAIL_names'] and not r['test_SKIP_names']
 assert sorted(n for n in runs if '/' in n)==sorted(r['expected_child_RUN_names'])
 if 'result_sha256' in record: assert sha(p)==record['result_sha256']
 rows.append({'name':r['name'],'result':str(p),'result_sha256':sha(p),'raw_sha256':sha(out/'run.log'),'top_pass':len([n for n in passes if '/' not in n]),'child_pass':len([n for n in passes if '/' in n]),'source_packet':str(out.parent.parent),'retained_prior':r['name'] not in selection['authorized_command_names'],'equivalence_differences':sorted(differences)})
assert len(rows)==len({r['name'] for r in rows})==14
counts={mode:{'top':sum(r['top_pass'] for r in rows if r['name'].startswith(mode+'-')),'child':sum(r['child_pass'] for r in rows if r['name'].startswith(mode+'-'))} for mode in ['normal','race']}
assert counts=={'normal':{'top':139,'child':65},'race':{'top':139,'child':65}},counts
actual={}
for base,dirs,files in os.walk(W):
 dirs[:]=[d for d in dirs if d!='.git']
 for name in files:
  p=Path(base)/name;n=p.relative_to(W).as_posix()
  if n!='.git' and p.is_file(): actual[n]=sha(p)
assert actual==manifest
v={'status':'PASS','created_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'focused producer controls with explicit unchanged-input historical reuse','manifest_sha256':sha(B/'source-frozen.json'),'archive_sha256':sha(B/'source-frozen.tar.gz'),'source_files':len(manifest),'summary_sha256':sha(B/'controls/summary.json'),'effective_counts':counts,'fresh_commands':8,'retained_prior_commands':6,'results':rows,'source_frozen':True,'all_groups_empty':True,'actual_native_driver_executed':False,'q32_acceptance':False,'performance_or_graph_qualification':False}
p=D/'native-decision/root-native-source-guard-producer06-focused-verification.json';assert not p.exists();p.write_text(json.dumps(v,indent=2,sort_keys=True)+'\n')
print(json.dumps({'status':v['status'],'effective_counts':counts,'verification_sha256':sha(p),'all_groups_empty':True}))
