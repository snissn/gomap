if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import hashlib,json,pathlib,subprocess,shlex,time,sys
import runpy
_gate=runpy.run_path('/tmp/gomap-4994-trial14mixedc1-growth-admission-root-v2.py')
APPROVED=_gate['admit']()
p=pathlib.Path('/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1/growth-lifecycle-root-v2');assert not p.exists();p.mkdir()
# Literal root-accepted ownership survives a plan read/hash refusal.
nodes=[{'host': '192.168.0.111', 'image': 'sha256:5071d652bdb56e152ca9cdba581e88b00bf513e1b17ed9e3439e0cfb8ffcf9a5', 'node': 'node-a', 'root': '/home/mikers/gomap-4250-twohost-rf4trial14mixedc1/node-a', 'name': 'treedb-4250-rf4trial14mixedc1-node-a'}, {'host': '192.168.0.111', 'image': 'sha256:5071d652bdb56e152ca9cdba581e88b00bf513e1b17ed9e3439e0cfb8ffcf9a5', 'node': 'node-b', 'root': '/home/mikers/gomap-4250-twohost-rf4trial14mixedc1/node-b', 'name': 'treedb-4250-rf4trial14mixedc1-node-b'}, {'host': '192.168.0.185', 'image': 'sha256:718a79a455321acc6409436db4de44414ed8c4dd42e5ca684e9abf0a664889f8', 'node': 'node-c', 'root': '/home/mikers/gomap-4250-twohost-rf4trial14mixedc1/node-c', 'name': 'treedb-4250-rf4trial14mixedc1-node-c'}, {'host': '192.168.0.185', 'image': 'sha256:718a79a455321acc6409436db4de44414ed8c4dd42e5ca684e9abf0a664889f8', 'node': 'node-d', 'root': '/home/mikers/gomap-4250-twohost-rf4trial14mixedc1/node-d', 'name': 'treedb-4250-rf4trial14mixedc1-node-d'}]
pinned_cids={'node-a': 'ba61c08f4cbd60ac656c4701c79e5c2b2d5bd4a633ec6d7c093acac3af8da12a', 'node-b': 'cab138126f9e8a9a46daffca5a53c1a828144f9b19b85e42593e68817a81afd4', 'node-c': 'af2c3f89b590a547fa388e49cc8250571b55dd16405898d341706931d745678a', 'node-d': '6e1968565621d2b2457a4c917b7af73748b39a0f7d5922b4c36ec7ad65f95fa5'}
owned_nodes=list(nodes);errors=[];serial=0

def call(n,label,args):
 global serial
 serial+=1;cmd=['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','mikers@'+n['host'],shlex.join(args)]
 try:r=subprocess.run(cmd,capture_output=True,text=True,timeout=45)
 except subprocess.TimeoutExpired as e:
  raw=lambda v:v.decode(errors='replace') if isinstance(v,bytes) else (v or '')
  (p/f'{serial:03d}-{label}-{n["node"]}.json').write_text(json.dumps({'argv':cmd,'exit':None,'timed_out':True,'stdout':raw(e.stdout),'stderr':raw(e.stderr),'time':time.time()},indent=2)+'\n');raise
 (p/f'{serial:03d}-{label}-{n["node"]}.json').write_text(json.dumps({'argv':cmd,'exit':r.returncode,'stdout':r.stdout,'stderr':r.stderr,'time':time.time()},indent=2)+'\n');assert r.returncode==0,(label,r.stderr);return r.stdout

def inspect(n):
 x=json.loads(call(n,'inspect',['docker','inspect',n['name']]))[0];old=json.load(open('/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1/bootstrap-root-v1/'+{'node-a':'38','node-b':'40','node-c':'42','node-d':'44'}[n['node']]+'-final-state-'+n['node']+'.json'));v=json.loads(old['stdout']);v=v[0] if isinstance(v,list) else v
 assert x['Id']==v['Id']==pinned_cids[n['node']] and x['Name']=='/'+n['name'] and x['Image']==n['image'] and x['Config']['Labels']['treedb.fixed-cluster.run']=='rf4trial14mixedc1'
 h=x['HostConfig'];assert h['Memory']==h['MemorySwap']==2147483648 and h['NanoCpus']==2000000000 and h['RestartPolicy']['Name']=='no' and h['NetworkMode']=='host'
 expected={n['root']+'/config.json:/config.json:ro',n['root']+'/credentials:/credentials:ro',n['root']+'/data:/data',n['root']+'/raft:/raft'}
 if n['node']=='node-c':expected.add(n['root']+'/dataset:/dataset:ro')
 assert set(h['Binds'])==expected and set(h['Binds'])==set(v['HostConfig']['Binds'])
 assert not x['State']['OOMKilled'];return x

try:
 plan_raw=pathlib.Path('/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1/bootstrap-root-v1/plan.json').read_bytes();assert hashlib.sha256(plan_raw).hexdigest()=='a3910b818e7a94ce5e1fe3cc2181132fbd18bb17d8429f5b9af8e068aa45c1d2'
 plan=json.loads(plan_raw);assert plan['nodes']==nodes
 # Exactly pinned stopped voters are restarted; no initialization or qualification rerun.
 for n in nodes:
  x=inspect(n);assert not x['State']['Running'] and x['State']['ExitCode']==0
 for n in nodes:
  cid=inspect(n)['Id'];assert call(n,'start',['docker','start',cid]).strip()==cid;assert inspect(n)['State']['Running']
 try:r=subprocess.run(['python3','/tmp/gomap-4994-trial14mixedc1-growth-query-root-v2.py'],capture_output=True,text=True,timeout=780)
 except subprocess.TimeoutExpired as e:
  raw=lambda v:v.decode(errors='replace') if isinstance(v,bytes) else (v or '')
  (p/'collector.timeout.stdout').write_text(raw(e.stdout));(p/'collector.timeout.stderr').write_text(raw(e.stderr));(p/'collector.timed_out').write_text('780 seconds\n');raise
 (p/'collector.stdout').write_text(r.stdout);(p/'collector.stderr').write_text(r.stderr);(p/'collector.exit').write_text(str(r.returncode)+'\n');assert r.returncode==0,r.stdout+r.stderr
except Exception as e:errors.append(repr(e))
finally:
 # If the wrapper times out, the child collector may not finish its own cleanup.
 # Independently stop this new, exactly pinned write driver before voters; never retry an ambiguous operation.
 try:
  n=next(x for x in nodes if x['node']=='node-c')
  name='treedb-4250-rf4trial14mixedc1-query-write'
  probe="if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')\nimport subprocess,json; r=subprocess.run(['docker','inspect',"+repr(name)+"],capture_output=True,text=True); assert r.returncode==0 or 'no such' in r.stderr.lower(); print(r.stdout if r.returncode==0 else 'null')"
  xs=json.loads(call(n,'driver-fallback-inspect',['python3','-c',probe]))
  if xs is not None:
   x=xs[0];assert x['Name']=='/'+name and x['Image']=='sha256:718a79a455321acc6409436db4de44414ed8c4dd42e5ca684e9abf0a664889f8' and x['Config']['Labels']['treedb.fixed-cluster.run']=='rf4trial14mixedc1'
   h=x['HostConfig'];assert h['Memory']==h['MemorySwap']==2147483648 and h['NanoCpus']==2000000000 and h['RestartPolicy']['Name']=='no' and h['NetworkMode']=='host'
   assert set(h['Binds'])=={APPROVED['remote_root']+'/node-c/config.json:/config.json:ro',APPROVED['remote_root']+'/node-c/credentials:/credentials:ro',APPROVED['remote_root']+'/bootstrap-qualify.json:/bootstrap.json:ro'}
   if x['State']['Running']:call(n,'driver-fallback-stop',['docker','stop','--time','10',x['Id']])
   call(n,'driver-fallback-logs',['docker','logs',x['Id']])
   x=json.loads(call(n,'driver-fallback-final',['docker','inspect',x['Id']]))[0];assert not x['State']['Running'] and x['State']['ExitCode']==0 and not x['State']['OOMKilled']
 except Exception as e:errors.append('driver fallback: '+repr(e))
 for n in owned_nodes:
  try:
   x=inspect(n)
   if x['State']['Running']:call(n,'stop',['docker','stop','--time','30',x['Id']])
   x=inspect(n);assert not x['State']['Running'] and not x['State']['OOMKilled'] and x['State']['ExitCode']==0
  except Exception as e:errors.append(n['node']+': '+repr(e))
 (p/'result.json').write_text(json.dumps({'errors':errors,'state':'PASS_STOPPED' if not errors else 'FAIL_STOPPED_OR_STOP_FAILURE','stores_preserved':True},indent=2)+'\n')
print(json.dumps({'errors':errors,'stores_preserved':True}));sys.exit(bool(errors))
