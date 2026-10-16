import datetime, hashlib, json, os, re
from pathlib import Path
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');B=D/'native-main-integration/native-source-guard-producer06';W=D/'native-integrated'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):return json.loads(p.read_text())
m=read(B/'source-frozen.json');summary=read(B/'actual-controls/summary.json');plan=read(B/'actual-proposed-commands.json')
assert summary['all_authorized_pass'] and summary['all_groups_empty'] and summary['source_still_frozen'] and summary['git_identity_still_frozen']
rows=[]
for c in plan['commands']:
 out=B/'actual-controls'/c['name'];r=read(out/'result.json');raw=(out/'run.log').read_text()
 assert r['status']=='PASS' and r['exit_code']==0 and r['leader_joined'] and r['group_and_children_empty']
 assert not r['signals'] and not r['remaining_group_members'] and not r['remaining_direct_or_adopted_children']
 assert read(out/'source-before.json')==read(out/'source-after.json')==m
 assert sha(out/'run.log')==r['raw_log_sha256']
 runs=re.findall(r'^=== RUN\s+(\S+)',raw,re.M);passes=re.findall(r'^\s*--- PASS: (\S+)',raw,re.M)
 expected=c['required_top_level_tests']+[parent+'/'+child for parent,children in c['required_child_tests'].items() for child in children]
 assert sorted(runs)==sorted(passes)==sorted(expected) and len(expected)==7
 assert runs==r['test_RUN_names'] and passes==r['test_PASS_names']
 prebirth=re.findall(r'native-split prebirth G=\{Records:(\d+) Bytes:(\d+)\}',raw)
 paused=[line.strip() for line in raw.splitlines() if 'native-split genuine paused Finish position=' in line]
 assert len(prebirth)==6 and len(paused)==2
 assert all('Phase:8' in x and 'AccountingComplete:false' in x and 'ACK=0' in x for x in paused)
 assert all(int(r)==39842 for r,b in prebirth)
 constructor=re.findall(r'domain=selected-empty-constructor-admission-envelope row=(P\d+) geometry=\{Charge:(\d+) Complete:(\w+) Overflow:(\w+)\}',raw)
 assert len(constructor)==132 and all(c=='true' and o=='false' for _,_,c,o in constructor)
 assert all(sum(x[0]==f'P{i:02d}' for x in constructor)==6 for i in range(1,23))
 assert raw.count('noQ32Acceptance=true')==6
 assert not re.search(r'WARNING: DATA RACE|panic:|goleak:|--- FAIL:|--- SKIP:',raw)
 rows.append({'name':c['name'],'result_sha256':sha(out/'result.json'),'raw_sha256':sha(out/'run.log'),'top_pass':1,'child_pass':6,'prebirth_grants':[{'records':int(r),'bytes':int(b)} for r,b in prebirth],'paused_finish':paused,'complete_constructor_rows':len(constructor)})
assert len(rows)==2
actual={}
for base,dirs,files in os.walk(W):
 dirs[:]=[d for d in dirs if d!='.git']
 for name in files:
  p=Path(base)/name;n=p.relative_to(W).as_posix()
  if n!='.git' and p.is_file():actual[n]=sha(p)
assert actual==m
v={'status':'PASS','created_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source_manifest_sha256':sha(B/'source-frozen.json'),'summary_sha256':sha(B/'actual-controls/summary.json'),'all_groups_empty':True,'live_source_equal':True,'results':rows,'original_Q32_acceptance':False,'performance_qualification':False,'scope':'all six real shared-protocol arms with source-derived diagnostic grant'}
p=D/'native-decision/root-native-source-guard-producer06-actual-verification.json';assert not p.exists();p.write_text(json.dumps(v,indent=2,sort_keys=True)+'\n');print(json.dumps({'status':v['status'],'verification_sha256':sha(p),'normal_children':6,'race_children':6}))
