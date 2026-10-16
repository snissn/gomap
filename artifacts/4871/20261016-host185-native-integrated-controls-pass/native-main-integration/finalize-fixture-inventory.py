from pathlib import Path
import datetime,hashlib,json,os,subprocess,tarfile
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'native-integrated';B=D/'native-main-integration/resource-finish-fixture';A=D/'native-main-integration/fixture-inventory-final'
def h(b):return hashlib.sha256(b).hexdigest()
def dump(p,x):p.write_text(json.dumps(x,indent=2,sort_keys=True)+'\n')
def run(name,args):
 r=subprocess.run(args,cwd=W,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=180)
 (A/(name+'.log')).write_bytes(r.stdout);dump(A/(name+'.json'),dict(argv=args,exit_code=r.returncode,log_sha256=h(r.stdout)))
 assert r.returncode==0,(name,r.stdout.decode(errors='replace'))
 return r.stdout
def fh(p):
 s=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1048576),b''):s.update(b)
 return s.hexdigest()
assert fh(B/'source-frozen.json')=='a7ff76f853958270984056845d89b173b0690b208909eb9623dbfd980407d2d1'
I=json.loads((B/'source-frozen.json').read_text());assert all(fh(W/n)==v for n,v in I.items())
assert subprocess.check_output(['git','write-tree'],cwd=W).decode().strip()=='7354d5a24c03ea69f39489d33c2013ba0907f9d6'
prior=set(subprocess.check_output(['git','diff','--cached','--name-only','-z'],cwd=W).decode().split('\0')[:-1]);assert len(prior)==202
names=['TreeDB/internal/rootpublication/durable_root_transaction_work_callbacks_v6_test.go']
previous=json.loads((D/'native-main-integration/origin-inventory-final/source-final.json').read_text());assert set(previous)==set(I) and [n for n in I if previous[n]!=I[n]]==names
with tarfile.open(B/'source-frozen.tar.gz','r:gz') as tf:
 archive={m.name:hashlib.sha256(tf.extractfile(m).read()).hexdigest() for m in tf if m.isfile()}
assert archive==I
before_text=(B/'test-before.go').read_text();after_text=(B/'test-after.go').read_text();start='func TestPrimaryFinishResourceRefusalAndFailureCustodyV6(';end='func TestNativeTransactionCallbackErrorIdentityV6(';assert before_text.split(start)[0]==after_text.split(start)[0] and before_text.split(end)[1]==after_text.split(end)[1]
assert (W/names[0]).read_bytes()==(B/'test-after.go').read_bytes()
A.mkdir(exist_ok=False);(A/'tmp').mkdir();env=dict(os.environ,TMPDIR=str(A/'tmp'),PYTHONDONTWRITEBYTECODE='1',GIT_OPTIONAL_LOCKS='0')
policy='.github/ci/ci_impact.json';before=(W/policy).read_bytes();(A/'policy-before.json').write_bytes(before)
run('stage-frozen',['git','add','--',*names]);actual=set(run('staged-paths',['git','diff','--cached','--name-only','-z']).decode().split('\0')[:-1]);assert actual==prior|set(names) and len(actual)==202
run('staged-check',['git','diff','--cached','--check']);run('staged-stat',['git','diff','--cached','--stat']);tree_before=run('tree-before',['git','write-tree']).decode().strip()
run('refresh',['python3','.github/scripts/refresh_ci_impact_inventory.py']);after=(W/policy).read_bytes();(A/'policy-after.json').write_bytes(after)
old=json.loads(before);new=json.loads(after);keys=[k for k in set(old)|set(new) if old.get(k)!=new.get(k)];assert keys==['discovery_source_sha256'],keys
assert all(fh(W/n)==v for n,v in I.items() if n!=policy)
run('source-contract',['python3','.github/scripts/refresh_ci_impact_inventory.py','--check'])
reuse={}
for name in ['test_refresh_ci_impact_inventory','test_ci_impact','test_treedb_ci_contract','test_treedb_windows_core_headroom']:
 p=D/'native-main-integration/origin-inventory-final'/(name+'.json');r=json.loads(p.read_text());assert r['exit_code']==0;assert fh(p.with_suffix('.log'))==r['log_sha256'];reuse[name]={'receipt':str(p),'receipt_sha256':fh(p),'unchanged_script_and_tests':True}
dump(A/'unchanged-python-check-reuse.json',reuse)
run('stage-policy',['git','add','--',policy]);run('complete-staged-check',['git','diff','--cached','--check']);tree_after=run('tree-after',['git','write-tree']).decode().strip()
assert subprocess.check_output(['git','diff','--name-only'],cwd=W)==b''
assert subprocess.check_output(['git','ls-files','--others','--exclude-standard'],cwd=W)==b''
I[policy]=h(after);dump(A/'source-final.json',I)
with tarfile.open(A/'source-final.tar.gz','w:gz',compresslevel=1) as tf:
 for n in sorted(I):tf.add(W/n,arcname=n,recursive=False)
receipt=dict(at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),base_manifest_sha256=fh(B/'source-frozen.json'),source_manifest_sha256=fh(A/'source-final.json'),source_archive_sha256=fh(A/'source-final.tar.gz'),source_files=len(I),source_delta=[policy],policy_before_sha256=h(before),policy_after_sha256=h(after),changed_policy_fields=keys,tree_before=tree_before,tree_after=tree_after,staged_path_count=len(actual),source_changes_beyond_policy=False,go_runtime_executed=False)
dump(A/'receipt.json',receipt);print(json.dumps(receipt,indent=2))
