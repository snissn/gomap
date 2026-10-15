from pathlib import Path
import hashlib,json,os,subprocess,time
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'m0-observation-harness';A=D/'m0-observation-preparation';R=A/'root-integration';R.mkdir(exist_ok=False)
f=json.loads((A/'freeze.json').read_text());changed=json.loads((A/'changed-files.json').read_text())
assert hashlib.sha256((A/'freeze.json').read_bytes()).hexdigest()=='8faa4d1d02a76f17e68ba9bc26c9e3692d50ad3fbd65dc660bf81861716a7e45'
for p,h in f['all_source_hashes'].items():assert hashlib.sha256((W/p).read_bytes()).hexdigest()==h,p
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=W,text=True).strip()==f['head']
assert not subprocess.check_output(['git','diff','--name-only'],cwd=W,text=True)
subprocess.run(['git','add','--',*changed],cwd=W,check=True)
assert set(subprocess.check_output(['git','diff','--cached','--name-only'],cwd=W,text=True).splitlines())==set(changed)
policy=W/'.github/ci/ci_impact.json';before=json.loads(policy.read_text());(R/'policy-before.json').write_text(json.dumps(before,indent=2)+'\n')
env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',TMPDIR=str(D/'tmp'),M0_TEST_PRODUCT=str(D/'ordinary-repair'))
cmd=['python3','.github/scripts/refresh_ci_impact_inventory.py'];result=subprocess.run(cmd,cwd=W,env=env,capture_output=True,text=True,check=True);(R/'refresh.txt').write_text(result.stdout+result.stderr)
x=json.loads(policy.read_text());new=[m for m in x['members'] if m['id'] not in {m['id'] for m in before['members']}];assert len(new)==1 and new[0]['workflow']=='treedb-vacuum-m0-observation.yml',new
new[0]['owner']='CI maintainer with affected M0 harness owner'
workflow='treedb-vacuum-m0-observation.yml'
for rule in x['rules']:
 if rule.get('full') or rule['glob']=='docs/**':rule['consumers']=sorted(set(rule['consumers']+[workflow]))
x['dynamic_consumers'].append({'source':'.github/scripts/mvcc_m0_observation.py','inputs':['**'], 'behavior':'Manual diagnostic pins product2588c, archives and hashes all tracked product/harness inputs, imports the checked candidate observer and applies a checked test overlay; no omission or qualification authority.'})
x['coverage_gaps'].append('M0 observation is manual diagnostic-only on a separately fixed product; no forecast starts it or replaces failed M0 acceptance.')
policy.write_text(json.dumps(x,indent=2,sort_keys=True)+'\n')
subprocess.run(['git','add','--','.github/ci/ci_impact.json'],cwd=W,check=True)
commands=[['python3','.github/scripts/refresh_ci_impact_inventory.py','--check'],['python3','.github/scripts/test_refresh_ci_impact_inventory.py'],['python3','.github/scripts/test_ci_impact.py'],['python3','.github/scripts/test_treedb_ci_contract.py'],['python3','.github/scripts/test_treedb_windows_core_headroom.py'],['python3','.github/scripts/test_mvcc_m0_observation.py']]
receipts=[]
for i,cmd in enumerate(commands):
 start=time.monotonic();r=subprocess.run(cmd,cwd=W,env=env,capture_output=True,text=True,timeout=120);p=R/(str(i)+'.txt');p.write_text(r.stdout+r.stderr)
 receipts.append({'argv':cmd,'exit':r.returncode,'seconds':time.monotonic()-start,'output':p.name,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()});(R/'commands.json').write_text(json.dumps(receipts,indent=2)+'\n')
 assert r.returncode==0,(cmd,r.stdout[-3000:],r.stderr[-3000:])
subprocess.run(['git','diff','--cached','--check'],cwd=W,check=True)
assert set(subprocess.check_output(['git','diff','--cached','--name-only'],cwd=W,text=True).splitlines())==set(changed)|{'.github/ci/ci_impact.json'}
for p,h in f['all_source_hashes'].items():
 if p!='.github/ci/ci_impact.json':assert hashlib.sha256((W/p).read_bytes()).hexdigest()==h,p
(R/'source-final.json').write_text(json.dumps({p:hashlib.sha256((W/p).read_bytes()).hexdigest() for p in f['all_source_hashes']},indent=2)+'\n')
(R/'staged.diff').write_bytes(subprocess.check_output(['git','diff','--cached','--binary'],cwd=W))
print(json.dumps({'files':7,'workflows':len(x['workflows']),'members':len(x['members']),'controls':len(receipts),'all_pass':True,'new_member':new}))
