from pathlib import Path
import datetime, hashlib, json, os, subprocess, tarfile
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'native-integrated';B=D/'native-build/ordinary-integrated-source';A=D/'native-main-integration/inventory'
A.mkdir(exist_ok=False);(A/'tmp').mkdir()
def h(b):return hashlib.sha256(b).hexdigest()
def dump(p,x):p.write_text(json.dumps(x,indent=2,sort_keys=True)+'\n')
def run(name,args):
 r=subprocess.run(args,cwd=W,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=180)
 (A/(name+'.log')).write_bytes(r.stdout);dump(A/(name+'.json'),dict(argv=args,exit_code=r.returncode,log_sha256=h(r.stdout)))
 assert r.returncode==0,(name,r.stdout.decode(errors='replace'))
 return r.stdout
env=dict(os.environ,TMPDIR=str(A/'tmp'),PYTHONDONTWRITEBYTECODE='1',GIT_OPTIONAL_LOCKS='0')
I=json.loads((B/'source-frozen.json').read_text());assert all(h((W/n).read_bytes())==v for n,v in I.items())
assert subprocess.check_output(['git','diff','--cached','--name-only'],cwd=W)==b''
changed=json.loads((B/'changed-files.json').read_text());names=[x['path'] for x in changed];assert len(names)==197
policy='.github/ci/ci_impact.json';before=(W/policy).read_bytes();(A/'policy-before.json').write_bytes(before)
run('stage-frozen',['git','add','--',*names])
actual=run('staged-paths',['git','diff','--cached','--name-only','-z']).decode().split('\0')[:-1];assert set(actual)==set(names)
run('staged-check',['git','diff','--cached','--check'])
run('staged-stat',['git','diff','--cached','--stat'])
tree_before=run('tree-before',['git','write-tree']).decode().strip()
run('refresh',['python3','.github/scripts/refresh_ci_impact_inventory.py'])
after=(W/policy).read_bytes();(A/'policy-after.json').write_bytes(after)
old=json.loads(before);new=json.loads(after);keys=[k for k in set(old)|set(new) if old.get(k)!=new.get(k)]
assert keys==['discovery_source_sha256'],keys
assert all(h((W/n).read_bytes())==v for n,v in I.items() if n!=policy)
run('source-contract',['python3','.github/scripts/refresh_ci_impact_inventory.py','--check'])
for name in ['test_refresh_ci_impact_inventory','test_ci_impact','test_treedb_ci_contract','test_treedb_windows_core_headroom']:
 run(name,['python3','.github/scripts/'+name+'.py'])
run('stage-policy',['git','add','--',policy])
run('complete-staged-check',['git','diff','--cached','--check'])
tree_after=run('tree-after',['git','write-tree']).decode().strip()
assert subprocess.check_output(['git','diff','--name-only'],cwd=W)==b''
I[policy]=h(after);dump(A/'source-final.json',I)
with tarfile.open(A/'source-final.tar.gz','w:gz',compresslevel=1) as tf:
 for n in sorted(I):tf.add(W/n,arcname=n,recursive=False)
def fh(p):
 s=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1048576),b''):s.update(b)
 return s.hexdigest()
receipt=dict(at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),base_manifest_sha256=h((B/'source-frozen.json').read_bytes()),source_manifest_sha256=fh(A/'source-final.json'),source_archive_sha256=fh(A/'source-final.tar.gz'),source_files=len(I),source_delta=[policy],policy_before_sha256=h(before),policy_after_sha256=h(after),changed_policy_fields=keys,tree_before=tree_before,tree_after=tree_after,staged_path_count=198,source_changes_beyond_policy=False,go_runtime_executed=False)
dump(A/'receipt.json',receipt);print(json.dumps(receipt,indent=2))
