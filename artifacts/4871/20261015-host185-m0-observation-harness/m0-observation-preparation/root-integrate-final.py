from pathlib import Path
import hashlib,json,os,subprocess,time
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'m0-observation-harness';A=D/'m0-observation-preparation';R=A/'root-integration-final';R.mkdir(exist_ok=False)
f=json.loads((A/'freeze.json').read_text());changed=json.loads((A/'changed-files.json').read_text())
expected=set(changed)|{'.github/ci/ci_impact.json','.github/scripts/test_ci_impact.py','docs/CI_IMPACT_SELECTION.md'}
assert set(subprocess.check_output(['git','diff','--cached','--name-only'],cwd=W,text=True).splitlines())==expected
assert not subprocess.check_output(['git','diff','--name-only'],cwd=W,text=True)
for p,h in f['all_source_hashes'].items():
 if p not in expected:assert hashlib.sha256((W/p).read_bytes()).hexdigest()==h,p
for p in changed:assert hashlib.sha256((W/p).read_bytes()).hexdigest()==f['all_source_hashes'][p],p
env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',TMPDIR=str(D/'tmp'),M0_TEST_PRODUCT=str(D/'ordinary-repair'))
commands=[['python3','.github/scripts/refresh_ci_impact_inventory.py','--check'],['python3','.github/scripts/test_refresh_ci_impact_inventory.py'],['python3','.github/scripts/test_ci_impact.py'],['python3','.github/scripts/test_treedb_ci_contract.py'],['python3','.github/scripts/test_treedb_windows_core_headroom.py'],['python3','.github/scripts/test_mvcc_m0_observation.py']]
receipts=[]
for i,cmd in enumerate(commands):
 start=time.monotonic();r=subprocess.run(cmd,cwd=W,env=env,capture_output=True,text=True,timeout=120);p=R/(str(i)+'.txt');p.write_text(r.stdout+r.stderr)
 receipts.append({'argv':cmd,'exit':r.returncode,'seconds':time.monotonic()-start,'output':p.name,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()});(R/'commands.json').write_text(json.dumps(receipts,indent=2)+'\n')
 assert r.returncode==0,(cmd,r.stdout[-3000:],r.stderr[-3000:])
subprocess.run(['git','diff','--cached','--check'],cwd=W,check=True)
files=subprocess.check_output(['git','ls-files','-z'],cwd=W).decode().split('\0');files=[p for p in files if p]
manifest={p:hashlib.sha256((W/p).read_bytes()).hexdigest() for p in files}
(R/'source-final.json').write_text(json.dumps(manifest,indent=2)+'\n')
(R/'staged.diff').write_bytes(subprocess.check_output(['git','diff','--cached','--binary'],cwd=W))
x=json.loads((W/'.github/ci/ci_impact.json').read_text())
print(json.dumps({'files':len(expected),'source_files':len(manifest),'workflows':len(x['workflows']),'members':len(x['members']),'controls':len(receipts),'all_pass':True}))
