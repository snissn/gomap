"""Offline RF3/RF4 orchestration and fail-stop contracts; no SSH or Docker."""
import contextlib, importlib.util, io, json, pathlib, shlex, sys, tempfile, types
from unittest.mock import patch
spec=importlib.util.spec_from_file_location('cluster4250',str(pathlib.Path(sys.argv[1]) if len(sys.argv)>1 else pathlib.Path(__file__).with_name('treedb_fixed_cluster_2host.py')));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
for size in (3, 4):
 with tempfile.TemporaryDirectory(prefix='gomap4250check-') as temp:
  root=pathlib.Path(temp); secret='PRIVATE-KEY-CONTENT-MUST-NOT-BE-RECEIPT'; credentials={}
  for key,name in [('TrustRootsFile','ca.pem'),('CertificateFile','node.pem'),('PrivateKeyFile','node-key.pem')]:
   p=root/name;p.write_text(secret);credentials[key]=str(p)
  configs=[]
  for i,host in enumerate([m.HOSTS[0],m.HOSTS[0]]+[m.HOSTS[1]]*(size-2)):
   p=root/('n%d.json'%i);p.write_text(json.dumps(dict(NodeID='n%d'%i,ClusterID='fixture',Nodes=[{'ID':'n%d'%j} for j in range(size)],Catalog={'Peers':[{'ID':'n%d'%j} for j in range(size)]},Groups=[{'Peers':[{'ID':'n%d'%j} for j in range(size)]}],Credentials=credentials,VectorInitialization=dict(IndexDefinition={'field':'embedding','dimensions':2},MaxSourceRows=3))));configs.append(dict(host=host,config=str(p)))
  manifest=dict(image='sha256:'+'a'*64,binary='/treedb-fixed-peer',binary_sha256='b'*64,nodes=configs);mf=root/'manifest.json';mf.write_text(json.dumps(manifest))
  def invoke(fault=None,execute=True):
   out=root/('receipts-'+str(fault));calls=[];inspect_count=0
   def fake(argv,**kw):
    nonlocal inspect_count
    calls.append(argv)
    if argv[0]=='scp':return types.SimpleNamespace(returncode=0,stdout='',stderr='')
    assert argv[:3]==['ssh','-o','BatchMode=yes'];remote=shlex.split(argv[-1]);label=''
    if remote[0]=='mkdir':return types.SimpleNamespace(returncode=0,stdout='',stderr='')
    assert remote[0]=='docker';cmd=remote[1:];text='';rc=0
    if cmd[0]=='run':
     assert cmd[1]=='--pull=never';assert '--network=host' in cmd and '--memory=2g' in cmd and '--memory-swap=2g' in cmd and '--cpus=2' in cmd
     assert manifest['image'] in cmd and cmd[cmd.index('-expected-binary-sha256')+1]==manifest['binary_sha256']
     mode=cmd[cmd.index('-mode')+1]
     if mode=='inspect':
      inspect_count+=1;text=json.dumps({'Config':{'Authenticated':True,'SharedSHA256':'different' if fault=='digest' and inspect_count==2 else 'shared'}})
      if fault in ['missing-image','binary-mismatch']:rc=125
     if mode in ['initialize','qualify']:
      assert cmd[cmd.index('-operation-timeout')+1]=='120s'
      if fault=='driver' and mode=='initialize':rc=1;text=json.dumps({'Stage':'seed','Error':'partial durable progress'})
      if fault=='timeout' and mode=='initialize':raise m.subprocess.TimeoutExpired(argv,180,output=b'partial')
    elif cmd[0]=='inspect':
     text=('wrong' if fault=='ownership' else 'checkrun') if 'Labels' in cmd[cmd.index('--format')+1] else ('137' if fault=='close' else '0')
    else:assert cmd[0] in ['stop','start','logs']
    return types.SimpleNamespace(returncode=rc,stdout=text,stderr='')
   argv=['cluster','--manifest',str(mf),'--run-id','checkrun']+(['--execute','--output',str(out)] if execute else [])
   error=None
   with patch.object(sys,'argv',argv),patch.object(m.subprocess,'run',fake),contextlib.redirect_stdout(io.StringIO()):
    try:m.main()
    except RuntimeError as e:error=str(e)
   return out,calls,error
  out,calls,error=invoke(execute=False);assert not calls and not out.exists() and error is None
  out,calls,error=invoke();assert error is None and json.loads((out/'result.json').read_text())['status']=='PASS'
  runs=[shlex.split(a[-1]) for a in calls if a[0]=='ssh' and shlex.split(a[-1])[:2]==['docker','run']];assert len(runs)==2*size+2
  assert sum('serve' in a for a in runs)==size and sum('inspect' in a for a in runs)==size
  for f in ['missing-image','binary-mismatch','digest','ownership','close','driver','timeout']:
   out,calls,error=invoke(f);assert error and not (out/'result.json').exists(),f
   assert all(secret not in p.read_text() for p in out.glob('*.json'))
   assert not any(any(token in shlex.split(a[-1]) for token in ['prune','rm','rmi','pull']) for a in calls if a[0]=='ssh')
   drivers=[a for a in calls if a[0]=='ssh' and '-mode initialize' in a[-1]];assert len(drivers)<=1
   if f in ['missing-image','binary-mismatch','digest']:assert not any('-mode serve' in a[-1] for a in calls)
   if f in ['ownership','close','driver','timeout']:assert not any('-mode qualify' in a[-1] for a in calls)
  bad=root/'duplicate.json';bad.write_text('{"a":1,"a":2}')
  try:m.read_json(bad);raise AssertionError('duplicate accepted')
  except ValueError:pass
  for changes in [dict(image='mutable:latest'),dict(binary_sha256='invalid'),dict(nodes=configs[:2])]:
   try:m.plan(dict(manifest,**changes),'checkrun');raise AssertionError('invalid manifest accepted')
   except ValueError:pass
  # Exact immutable per-host image IDs remain supported for both layouts.
  per_host=dict(manifest,image={host:'sha256:'+('c' if host==m.HOSTS[0] else 'd')*64 for host in m.HOSTS})
  planned=m.plan(per_host,'checkrun');assert len(planned)==size and all(n['image']==per_host['image'][n['host']] for n in planned)
  for changes in [dict(image={m.HOSTS[0]:'sha256:'+'a'*64}),dict(nodes=[dict(n,host=m.HOSTS[0]) for n in configs])]:
   try:m.plan(dict(manifest,**changes),'checkrun');raise AssertionError('invalid host mapping accepted')
   except ValueError:pass
  if size==4:
   misplaced=dict(manifest,nodes=[dict(n,host=m.HOSTS[0] if i<3 else m.HOSTS[1]) for i,n in enumerate(configs)])
   try:m.plan(misplaced,'checkrun');raise AssertionError('RF4 3+1 server placement accepted')
   except ValueError:pass
  # Matching config identity must still name every actual catalog/data voter.
  first=pathlib.Path(configs[0]['config']);original=first.read_text();invalid=json.loads(original);invalid['Catalog']['Peers']=invalid['Catalog']['Peers'][:-1];first.write_text(json.dumps(invalid))
  try:m.plan(manifest,'checkrun');raise AssertionError('missing catalog voter accepted')
  except ValueError:pass
  first.write_text(original)
  print(json.dumps({'status':'PASS','scope':'offline orchestrator argv, RF3/RF4 lifecycle, 7 fail-stop cases, no execution, immutable manifest and duplicate guards','server_nodes':size,'docker_runs':2*size+2,'real_ssh_docker':False}))
