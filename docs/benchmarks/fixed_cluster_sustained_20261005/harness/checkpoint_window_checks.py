"""Actual checkpoint admission/permission source controls, never qualification."""
if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')

def checkpoint_controls():
 import time,os,stat,io,contextlib
 from types import SimpleNamespace
 root=fixtures/'checkpoint-window-controls';root.mkdir()
 c=load(R/m['roles']['collector']['output']['path'],'checkpoint_actual_collector')
 p=load(R/m['roles']['permission_stage']['output']['path'],'checkpoint_actual_permission')
 # All historical schemas below are synthetic. Only this inert test module's
 # pins are rebound; the production collector's retained pins remain unchanged.
 raw={}
 a=c.manifest_template()
 a.update(synthetic_only=True,Version=1,RunID=c.RUN,remote_root=c.ROOT,
  source_head=c.CHECKPOINT_RUNTIME[0],source_tree=c.CHECKPOINT_RUNTIME[1],
  source_inventory_sha256='a'*64,query_image='sha256:'+'b'*64,
  driver_sha256='c'*64,driver_uid_gid='1000:1000',bootstrap_sha256='d'*64,
  config_sha256='e'*64,plan_sha256='f'*64,input_inventory_sha256='a'*64,
  baseline={'PopulationRows':10005,'PopulationSHA256':'b'*64,'HighestCommitIndex':93,'CorpusRows':10000,'AnchorRows':5,'Dimensions':128,'Queries':16},
  nodes=[{'node':node,'host':host,'image':'sha256:'+'b'*64,'server_sha256':'c'*64,'cid':str(i+1)*64} for i,(node,host) in enumerate(c.HOSTS.items())],
  receipts={},local_pins={})
 a.update(dict.fromkeys(c.FLAGS,False))
 c.CHECKPOINT_ALLOWED=True
 c.CHECKPOINT_HARNESS=('7'*40,'8'*40)
 def pin(name,obj):
  path=root/(name+'.json');b=json.dumps(obj,indent=2).encode();path.write_bytes(b)
  raw[str(path)]=b;a['local_pins'][str(path)]=sha(b)
  return {'path':str(path),'sha256':sha(b)}
 for key in ('source_inventory','build','source_prereview','bootstrap','config','plan','baseline','initial_oracle','prefix_oracles'):
  a['receipts'][key]=pin('synthetic-unchanged-'+key,{'synthetic_only':True,'receipt':key})['path']
 refs={}
 refs['decision']=pin('synthetic-original-decision',{'synthetic_only':True,'original_campaign':{'state':'INCOMPLETE_RETIRED_AFTER_NO_RPC_PERMISSION_SETUP_REFUSAL'},'future_phase':{'phase_id':c.CHECKPOINT_PHASE}})
 refs['original_predeclaration']=pin('synthetic-original-predeclaration',{'synthetic_only':True,'campaign':c.RUN,'accepted_workload':c.W.declaration(),'runtime_started':False})
 refs['original_budget']=pin('synthetic-original-budget',{'synthetic_only':True,'predeclaration_sha256':refs['original_predeclaration']['sha256']})
 refs['original_inactive']=pin('synthetic-original-inactive',copy.deepcopy(a))
 refs['refusal']=pin('synthetic-original-refusal',{'synthetic_only':True,'state':'ACTUAL_ISOLATED_PERMISSION_BOUNDS_REFUSAL_NO_WINDOW_DISPATCH','reported_measured_attempts':0,'reported_warmup_attempts':0,'window_output_exists':False,'all_nine_activation_flags_false':True})
 refs['native']=pin('synthetic-original-native',{'synthetic_only':True,'original_budget_sha256':refs['original_budget']['sha256'],'promoted_oracle_sha256':a['local_pins'][a['receipts']['prefix_oracles']]})
 refs['retention']=pin('synthetic-original-retention',{'synthetic_only':True,'state':'THREE_HASH_VERIFIED_CHECKPOINT_COPIES_NOT_WINDOW_QUALIFICATION','copies_count':3,'qualification_accepted':False})
 production_pins=dict(c.CHECKPOINT_PINS)
 c.CHECKPOINT_PINS={key:row['sha256'] for key,row in refs.items()}
 assert set(c.CHECKPOINT_PINS)==set(production_pins)
 assert all(json.loads(raw[row['path']])['synthetic_only'] is True for row in refs.values())
 pre={'synthetic_only':True,'state':'PROSPECTIVE_CHECKPOINT_WINDOW_PREDECLARED','PhaseID':c.CHECKPOINT_PHASE,'campaign':c.RUN,'runtime_started':False,'accepted_workload':c.W.declaration(),'runtime_head':a['source_head'],'runtime_tree':a['source_tree'],'harness_head':c.CHECKPOINT_HARNESS[0],'harness_tree':c.CHECKPOINT_HARNESS[1],'checkpoint_refs':refs}
 pr=pin('synthetic-phase-predeclaration',pre);a['receipts']['predeclaration']=pr['path']
 now=time.time()
 budget={'synthetic_only':True,'state':'CHECKPOINT_WINDOW_PERMISSION_DISPATCH_STARTED','PhaseID':c.CHECKPOINT_PHASE,'budget_seconds':7200,'started_unix':now-1,'deadline_unix':now-1+7200,'predeclaration_sha256':pr['sha256'],'graph_decision_sha256':c.CHECKPOINT_PINS['decision']}
 br=pin('synthetic-phase-budget',budget)
 unused={'synthetic_only':True,'state':'FRESH_ORIGINAL_WINDOW_UNUSED','PhaseID':c.CHECKPOINT_PHASE,'original_inactive_sha256':c.CHECKPOINT_PINS['original_inactive'],'output':str(c.OUTPUT),'name':c.NAME,'gate':c.GATE,'output_absent':True,'driver_absent':True,'gate_absent':True,'no_issued_mutations':True,'all_four_stopped_owned':True,'stopped_nodes':{n['node']:n['cid'] for n in a['nodes']},'observed_unix':now-2}
 records=[]
 producer_calls=[]
 def observe(host,case=None):
  command=c.checkpoint_unused_argv(a,host);args=__import__('shlex').split(command[-1])
  payload=json.loads(args[-1]);calls=[]
  def inspect_stub(argv,**kwargs):
   assert argv[:2]==['docker','inspect'] and len(argv)==3 and kwargs['timeout']==20
   calls.append(argv)
   if argv[-1]==c.NAME:
    return SimpleNamespace(returncode=0 if case=='driver_present' else 1,stdout='[]',stderr='no such container')
   node=next(v for v in payload['nodes'] if v['cid']==argv[-1])
   state=dict(Running=case=='running',ExitCode=1 if case=='bad_exit' else 0,OOMKilled=case=='oom')
   row=dict(Id='f'*64 if case=='foreign_cid' else node['cid'],Image='sha256:'+'f'*64 if case=='foreign_image' else node['image'],State=state)
   return SimpleNamespace(returncode=0,stdout=json.dumps([row]),stderr='')
  ns=dict(json=json,sys=SimpleNamespace(argv=['-c',args[-1]]),subprocess=SimpleNamespace(run=inspect_stub),pathlib=SimpleNamespace(Path=lambda v:SimpleNamespace(exists=lambda:case=='gate_present',is_symlink=lambda:case=='gate_symlink')))
  tree=ast.parse(args[2]);tree.body=[v for v in tree.body if not isinstance(v,(ast.Import,ast.ImportFrom))]
  output=io.StringIO()
  with contextlib.redirect_stdout(output):exec(compile(tree,'actual-canonical-unused-observer','exec'),ns)
  assert len(calls)==(3 if host==c.HOST else 2)
  producer_calls.append(dict(host=host,argv=command,docker_inspect_calls=calls,stdout=output.getvalue(),actual_network_calls=0))
  return output.getvalue()
 for host in sorted(set(c.HOSTS.values())):
  host='mikers@'+host;stdout=observe(host)
  records.append(pin('synthetic-unused-raw-'+host.split('@')[1],{'synthetic_only':True,'argv':c.checkpoint_unused_argv(a,host),'exit_code':0,'started_unix':now-4,'finished_unix':now-3,'stdout':stdout,'stderr':'','stdout_sha256':sha(stdout.encode()),'stderr_sha256':sha(b''),'timeout_seconds':90}))
 unused['raw_evidence']=records;ur=pin('synthetic-unused-proof',unused)
 auth={'synthetic_only':True,'state':'AUTHORIZED_SINGLE_FIRST_CHECKPOINT_WINDOW','PhaseID':c.CHECKPOINT_PHASE,'RunID':c.RUN,'runtime_head':a['source_head'],'runtime_tree':a['source_tree'],'harness_head':c.CHECKPOINT_HARNESS[0],'harness_tree':c.CHECKPOINT_HARNESS[1],'collector_sha256':a['collector_sha256'],'predeclaration_sha256':pr['sha256'],'all_cleanup_io_stopped':True,'no_other_campaign_or_writer':True,'retained_owned_stores_verified':True,'phase_budget':br,'unused_window_proof':ur}
 ar=pin('synthetic-auth',auth);a['receipts']['run_authorization']=ar['path']
 common={'synthetic_only':True,'harness_head':c.CHECKPOINT_HARNESS[0],'harness_tree':c.CHECKPOINT_HARNESS[1],'runtime_head':a['source_head'],'runtime_tree':a['source_tree'],'collector_sha256':a['collector_sha256']}
 lr=pin('synthetic-harness-landed',dict(common,state='LANDED_CHECKPOINT_WINDOW_HARNESS_VERIFIED',required_ci_passed=True,merge_commit='9'*40))
 cr=pin('synthetic-collector-review',dict(common,decision='ACCEPT',findings=[],predeclaration_sha256=pr['sha256']))
 a['receipts'].update(landed_source=lr['path'],collector_prereview=cr['path'])
 c.validate_checkpoint_window(a,raw,pre);checks.append('checkpoint_actual_admission_exact_retained_tuple_positive')
 c.CHECKPOINT_ALLOWED=False
 bad('checkpoint_fresh_default_cannot_admit_checkpoint',lambda:c.validate_checkpoint_window(a,raw,pre))
 c.CHECKPOINT_ALLOWED=True
 assert c.checkpoint_budget(a,raw)==budget;checks.append('checkpoint_actual_runtime_budget_positive')
 for label in ('missing_host','duplicate_host','foreign_host','substituted_cid','driver_present','missing_union'):
  uu=copy.deepcopy(unused);aa=copy.deepcopy(a);changed=dict(raw)
  if label=='missing_host':uu['raw_evidence']=uu['raw_evidence'][:1]
  elif label=='duplicate_host':uu['raw_evidence']=[uu['raw_evidence'][0]]*2
  elif label=='missing_union':uu['stopped_nodes'].pop('node-a')
  else:
   row=uu['raw_evidence'][-1];rr=json.loads(changed[row['path']]);obs=json.loads(rr['stdout'])
   if label=='foreign_host':rr['argv'][5]='mikers@192.168.0.200'
   elif label=='substituted_cid':obs['stopped_nodes'][next(iter(obs['stopped_nodes']))]='1'*64
   else:obs['driver_absent']=False
   rr['stdout']=json.dumps(obs);rr['stdout_sha256']=sha(rr['stdout'].encode());changed[row['path']]=json.dumps(rr).encode();row['sha256']=sha(changed[row['path']]);aa['local_pins'][row['path']]=row['sha256']
  changed[ur['path']]=json.dumps(uu).encode();aa['local_pins'][ur['path']]=sha(changed[ur['path']]);zz=copy.deepcopy(auth);zz['unused_window_proof']['sha256']=aa['local_pins'][ur['path']];changed[ar['path']]=json.dumps(zz).encode()
  bad('checkpoint_actual_unused_two_host_reject_'+label,lambda aa=aa,changed=changed:c.validate_checkpoint_window(aa,changed,pre))
 for key in ('source_head','source_tree','input_inventory_sha256','config_sha256','bootstrap_sha256','plan_sha256','driver_sha256','query_image'):
  z=copy.deepcopy(a);z[key]='sha256:'+'1'*64 if key=='query_image' else '1'*(40 if key in ('source_head','source_tree') else 64)
  bad('checkpoint_reject_substituted_'+key,lambda z=z:c.validate_checkpoint_window(z,raw,pre))
 for key in ('native','retention','decision','original_budget','original_inactive','original_predeclaration','refusal'):
  z=copy.deepcopy(pre);z['checkpoint_refs'][key]['sha256']='1'*64
  bad('checkpoint_reject_substituted_'+key+'_receipt',lambda z=z:c.validate_checkpoint_window(a,raw,z))
 z=copy.deepcopy(pre);z['accepted_workload']['Originals']=58
 bad('checkpoint_reject_stale_workload',lambda:c.validate_checkpoint_window(a,raw,z))
 z=copy.deepcopy(a);z['receipts']['predeclaration']=c.PREDECLARATION
 bad('checkpoint_reject_original_predeclaration_reuse',lambda:c.validate_checkpoint_window(z,raw,pre))
 for key in ('phase_budget','unused_window_proof'):
  z=copy.deepcopy(auth);del z[key];changed=dict(raw);changed[ar['path']]=json.dumps(z).encode()
  bad('checkpoint_reject_missing_'+key,lambda changed=changed:c.validate_checkpoint_window(a,changed,pre))
 for key,value in [('state','AUTHORIZED_SINGLE_FRESH_CAMPAIGN'),('runtime_head','1'*40),('harness_head','1'*40),('retained_owned_stores_verified',False)]:
  z=copy.deepcopy(auth);z[key]=value;changed=dict(raw);changed[ar['path']]=json.dumps(z).encode()
  bad('checkpoint_reject_authorization_'+key,lambda changed=changed:c.validate_checkpoint_window(a,changed,pre))
 for key,value in [('budget_seconds',7199),('budget_seconds',True),('deadline_unix',now+7201),('graph_decision_sha256','1'*64)]:
  z=copy.deepcopy(budget);z[key]=value;changed=dict(raw);changed[br['path']]=json.dumps(z).encode();aa=copy.deepcopy(a);aa['local_pins'][br['path']]=sha(changed[br['path']]);zz=copy.deepcopy(auth);zz['phase_budget']['sha256']=aa['local_pins'][br['path']];changed[ar['path']]=json.dumps(zz).encode()
  bad('checkpoint_reject_incomplete_budget_'+key+'_'+repr(value),lambda aa=aa,changed=changed:c.validate_checkpoint_window(aa,changed,pre))
 assert c.phase_timeout(budget,45)<=45
 expired=dict(budget,deadline_unix=now-1)
 try:c.phase_timeout(expired,45)
 except AssertionError:checks.append('checkpoint_expired_budget_refuses_new_dispatch')
 else:raise AssertionError('expired phase admitted')
 # Actual finally opts in only the bounded ownership/stop/evidence calls.
 main=next(n for n in ast.parse(Path(c.__file__).read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name=='main')
 final=next(n for n in main.body if isinstance(n,ast.Try) and n.finalbody)
 assert 'phase_cleanup' not in Path(c.__file__).read_text()
 final_calls=[v for v in ast.walk(ast.Module(body=final.finalbody,type_ignores=[])) if isinstance(v,ast.Call) and isinstance(v.func,ast.Name) and v.func.id=='call']
 assert final_calls and all(any(k.arg=='closure' and isinstance(k.value,ast.Constant) and k.value.value is True for k in v.keywords) for v in final_calls)
 checks.append('checkpoint_actual_finally_preserves_bounded_cleanup')
 saved_run=subprocess.run
 calls=[];cleanup=root/'synthetic-cleanup-ledger';cleanup.mkdir()
 c.OUTPUT=cleanup;c.serial=0;c.deadline=None;c.phase_budget=expired
 def no_transport(argv,**kwargs):calls.append(argv);return SimpleNamespace(returncode=0,stdout=b'synthetic-cleanup-only',stderr=b'')
 subprocess.run=no_transport
 try:
  try:c.call('new-dispatch',['synthetic-no-execution'])
  except AssertionError:pass
  else:raise AssertionError('expired dispatch did not refuse')
  assert calls==[] and list(cleanup.iterdir())==[];checks.append('checkpoint_actual_expired_call_refuses_before_transport_or_writes')
  assert c.call('driver-stopped',['docker','inspect','a'*64],closure=True)=='synthetic-cleanup-only'
  assert len(calls)==1 and len(list(cleanup.iterdir()))==3;checks.append('checkpoint_actual_exhausted_cleanup_retains_bounded_inspect')
 finally:
  subprocess.run=saved_run;c.phase_budget=None;c.OUTPUT=Path(c.W.output)
 argv=p.isolated_arguments(c,a,'a'*32);name=c.NAME+'-'+c.CHECKPOINT_PHASE+'-isolated-permission'
 assert argv[argv.index('--name')+1]==name and len([v for v in argv if v=='-v'])==4
 assert argv[argv.index('-run-id')+1]==c.QUERY_RUN and argv[argv.index('-read-window')+1]=='60s'
 checks.append('checkpoint_actual_permission_fresh_name_same_queries_four_readonly_binds')
 for key,value in [('--name',c.NAME+'-isolated-permission'),('-run-id','foreign')]:
  z=list(argv);z[z.index(key)+1]=value
  bad('checkpoint_actual_permission_reject_'+key,lambda z=z:p.validate_command(c,a,'a'*32,z))
 z=list(argv);z[z.index('-v')+1]='/foreign:/recall:ro'
 bad('checkpoint_actual_permission_reject_stale_bind',lambda:p.validate_command(c,a,'a'*32,z))
 fresh=dict(a,receipts=dict(a['receipts'],predeclaration=c.PREDECLARATION));assert p.permission_name(c,fresh)==c.NAME+'-isolated-permission'
 checks.append('checkpoint_preserves_fresh_campaign_permission_name')
 # Run the actual permission main with a transport stub that expires the phase
 # immediately after launch; no SSH/Docker is executed. Closure owns exact CID.
 for label in ('stopped','wait_timeout','foreign_final_cid'):
  folder=root/('actual-permission-closure-'+label);folder.mkdir()
  pm=load(R/m['roles']['permission_stage']['output']['path'],'checkpoint_permission_closure_'+label)
  pc=load(R/m['roles']['collector']['output']['path'],'checkpoint_permission_closure_collector_'+label)
  pc.CHECKPOINT_ALLOWED=True;pc.CHECKPOINT_HARNESS=c.CHECKPOINT_HARNESS
  pc.OUTPUT=folder/'unused-local-output';pc.LOCAL_INPUT_ROOT=str(folder/'frozen-inputs')
  pc.prepare_local=lambda value:(raw,{},[],[])
  keys=['inspect_path','command_path','stdout_path','stderr_path','launch_path','exit_path','inspect_receipt_path','logs_path']
  pm.load=lambda:(pc,lambda *args:None,None,keys)
  pm.HEAD=a['source_head'];pm.TREE=a['source_tree'];pm.MANIFEST=str(folder/'final-inactive-root-v1.json');pm.ARCHIVE=str(folder/'inputs.tar.gz')
  manifest=Path(pm.MANIFEST.replace('-root-v1.json','-'+c.CHECKPOINT_PHASE+'-root-v1.json'));manifest.write_text(json.dumps(a));Path(pm.ARCHIVE).write_bytes(b'synthetic-archive-not-runtime')
  pm.archive_check=lambda *args:None;pm.validate_permission=lambda *args:({'synthetic_only':True},'synthetic closure only')
  pc_time=pc.time;transport=[];running=label!='stopped';inspect_count=0
  cid='a'*64;nonce='b'*32;pm.secrets=SimpleNamespace(token_hex=lambda size:nonce)
  def permission_transport(command,**kwargs):
   nonlocal running,inspect_count
   transport.append(command);args=__import__('shlex').split(command[-1])
   stdout=b'PASS_SYNTHETIC_ONLY';stderr=b''
   if args[:2]==['docker','run']:
    pc.time=SimpleNamespace(time=lambda:budget['deadline_unix']+1);stdout=cid.encode()
   elif args[:2]==['docker','wait']:
    if label!='stopped':raise subprocess.TimeoutExpired(command,kwargs['timeout'])
    stdout=b'1'
   elif args[:2]==['docker','inspect']:
    inspect_count+=1
    row={'Id':('f'*64 if label=='foreign_final_cid' else cid),'Image':a['query_image'],'Name':'/'+name,'Config':{'User':a['driver_uid_gid'],'Labels':{'treedb.fixed-cluster.run':c.RUN,'treedb.fixed-cluster.invocation':nonce}},'State':{'Running':running}}
    stdout=json.dumps([row]).encode()
   elif args[:2]==['docker','stop']:running=False;stdout=cid.encode()
   return SimpleNamespace(returncode=0,stdout=stdout,stderr=stderr)
  pm.subprocess=SimpleNamespace(run=permission_transport,TimeoutExpired=subprocess.TimeoutExpired)
  opts=SimpleNamespace(manifest=str(manifest),manifest_sha256=sha(manifest.read_bytes()),archive=pm.ARCHIVE,archive_sha256=sha(Path(pm.ARCHIVE).read_bytes()),out=str(folder/'owned-ledger'))
  error=None
  try:
   with contextlib.redirect_stdout(io.StringIO()):pm.main(opts)
  except (subprocess.TimeoutExpired,ValueError) as e:error=type(e).__name__
  finally:pc.time=pc_time
  args=[__import__('shlex').split(v[-1]) for v in transport]
  if label=='stopped':assert error is None and inspect_count==2 and any(v[:2]==['docker','logs'] for v in args)
  elif label=='wait_timeout':assert error=='TimeoutExpired' and running is False and inspect_count==2 and any(v[:2]==['docker','stop'] for v in args)
  else:assert error=='ValueError' and running is True and not any(v[:2]==['docker','stop'] for v in args)
  (folder/'control-result.json').write_text(json.dumps({'state':'SYNTHETIC_NO_TRANSPORT','argv':transport,'error':error,'budget_expired_after_launch':True,'actual_network_calls':0})+'\n')
  checks.append('checkpoint_actual_permission_exhausted_closure_'+label)
 # Run the actual emitted read-only verifier against a bounded local tree.
 stage=root/'synthetic-existing-stage';stage.mkdir();(stage/'dataset').mkdir();(stage/'dataset'/'one').write_bytes(b'synthetic-not-runtime')
 inventory={'dataset/one':sha((stage/'dataset'/'one').read_bytes())};blob=json.dumps(inventory).encode();(stage/'input-inventory.json').write_bytes(blob)
 class FakePath:
  def __init__(self,value):self.p=Path(value.p if isinstance(value,FakePath) else value)
  def __truediv__(self,v):return FakePath(self.p/v)
  def __str__(self):return str(self.p)
  def __fspath__(self):return str(self.p)
  def __getattr__(self,k):
   if k=='parents':return [FakePath(v) for v in self.p.parents]
   if k=='stat':return lambda:SimpleNamespace(**dict((q,getattr(self.p.stat(),q)) for q in ('st_size','st_mode')),st_uid=1000,st_gid=1000)
   if k=='rglob':return lambda pattern:[FakePath(v) for v in self.p.rglob(pattern)]
   if k=='relative_to':return lambda other:self.p.relative_to(other.p if isinstance(other,FakePath) else other)
   return getattr(self.p,k)
 def verify_stage():
  ns={'__debug__':True};tree=ast.parse(p.VERIFY_STAGED);tree.body=[n for n in tree.body if not isinstance(n,(ast.Import,ast.ImportFrom))]
  ns.update(os=SimpleNamespace(getuid=lambda:1000,getgid=lambda:1000),sys=SimpleNamespace(argv=['verify',str(stage),sha(blob),json.dumps(inventory)]),pathlib=SimpleNamespace(Path=FakePath,PurePosixPath=__import__('pathlib').PurePosixPath),hashlib=hashlib,json=json,stat=stat)
  with contextlib.redirect_stdout(io.StringIO()):exec(compile(tree,'actual-read-only-existing-stage-verifier','exec'),ns)
 before={str(v):v.read_bytes() for v in stage.rglob('*') if v.is_file()};verify_stage()
 assert before=={str(v):v.read_bytes() for v in stage.rglob('*') if v.is_file()};checks.append('checkpoint_actual_existing_stage_verifier_read_only_positive')
 (stage/'extra').write_bytes(b'pollution');bad('checkpoint_actual_existing_stage_reject_extra',verify_stage)
 # Never remove prior evidence: each refusal uses a fresh separate tree.
 original_stage=stage
 for label in ('changed_digest','symlink','extra_directory'):
  stage=root/('synthetic-stage-'+label);stage.mkdir();(stage/'dataset').mkdir();(stage/'dataset'/'one').write_bytes(b'synthetic-not-runtime');(stage/'input-inventory.json').write_bytes(blob)
  if label=='changed_digest':(stage/'dataset'/'one').write_bytes(b'changed')
  elif label=='symlink':(stage/'alias').symlink_to(stage/'dataset'/'one')
  else:(stage/'empty').mkdir()
  bad('checkpoint_actual_existing_stage_reject_'+label,verify_stage)
 for optimization in ('-O','-OO'):
  command=[sys.executable,optimization,'-c',p.VERIFY_STAGED,str(root/'must-not-create'),sha(blob),json.dumps(inventory)]
  result=subprocess.run(command,capture_output=True,timeout=10)
  for suffix,data in [('stdout',result.stdout),('stderr',result.stderr),('exit',str(result.returncode).encode()),('argv.json',json.dumps(command).encode())]:(root/('verify-staged-'+optimization[1:]+'.'+suffix)).write_bytes(data)
  assert result.returncode!=0 and b'ordinary Python required' in result.stderr and not (root/'must-not-create').exists()
  checks.append('checkpoint_actual_existing_stage_optimized_'+optimization+'_refusal_before_inputs')
 (root/'checkpoint-integration.json').write_text(json.dumps({'state':'SOURCE_ONLY_SYNTHETIC_PHASE_NOT_ACCEPTANCE','synthetic_receipt_refs':refs,'production_receipt_pins_unchanged':production_pins,'phase_predeclaration':pr,'permission_argv':argv,'runtime_started':False,'network_calls':0,'limitations':['Every receipt is synthesized under the explicit fixture root; the inert test collector alone uses their hashes.','Harness review/landing/budget/unused observations are synthetic and never root acceptance.','Ownership of local verifier fixture is supplied by a UID/GID1000 stat facade; no remote files or runtime were accessed.']},indent=2)+'\n')
 # Complete actual source emission uses actual producer-shaped synthetic
 # runtime receipts and a separate synthetic harness envelope, never acceptance.
 hrows=[]
 for path in sorted(R.rglob('*')):
  if not path.is_file():continue
  b=path.read_bytes();hrows.append({'path':'docs/benchmarks/fixed_cluster_sustained_20261005/harness/'+path.relative_to(R).as_posix(),'mode':'100755' if path.stat().st_mode&0o111 else '100644','git_blob':hashlib.sha1(b'blob '+str(len(b)).encode()+b'\0'+b).hexdigest()})
 hi=pin('synthetic-harness-inventory',{'head':c.CHECKPOINT_HARNESS[0],'tree':c.CHECKPOINT_HARNESS[1],'rows':hrows,'overlays':{}})
 reviews=[pin('synthetic-harness-review-'+str(i),{'synthetic_only':True,'decision':'ACCEPT','candidate_head':c.CHECKPOINT_HARNESS[0],'candidate_tree':c.CHECKPOINT_HARNESS[1],'findings':[],'reviewer':'synthetic-checkpoint-reviewer-'+str(i)}) for i in range(2)]
 land=pin('synthetic-harness-landing',{'synthetic_only':True,'state':'LANDED_SOURCE_TREE_VERIFIED','runtime_head':c.CHECKPOINT_HARNESS[0],'runtime_tree':c.CHECKPOINT_HARNESS[1],'merge_commit':'9'*40,'source_inventory_sha256':hi['sha256']})
 ci=pin('synthetic-harness-ci',{'synthetic_only':True,'head':c.CHECKPOINT_HARNESS[0],'required_ci_passed':True,'checks':[{'name':'synthetic-only','conclusion':'SUCCESS','details_url':'https://github.com/synthetic-only/fixture/actions/runs/1'}]})
 acceptance=pin('synthetic-harness-acceptance',{'synthetic_only':True,'outcome':'ACCEPT','candidate_head':c.CHECKPOINT_HARNESS[0],'candidate_tree':c.CHECKPOINT_HARNESS[1],'source_inventory_sha256':hi['sha256'],'landed_source_verified':True,'required_ci_passed':True,'underlying_reviews':reviews,'landing_evidence':land,'ci_evidence':ci})
 # Reuse the checker's rich-build/projected-image schemas, with the fixed
 # runtime identity required by the actual checkpoint constructor.
 declaration=copy.deepcopy(d)
 runtime_head,runtime_tree=c.CHECKPOINT_RUNTIME
 declaration.update(source_head=runtime_head,source_tree=runtime_tree,source_root=str(root/'trial24-protected-product'))
 def runtime_receipt(key):
  value=json.loads(Path(d[key]['path']).read_bytes());value['synthetic_only']=True
  value.update(head=runtime_head,tree=runtime_tree)
  return value
 inventory_obj=runtime_receipt('source_inventory')
 declaration['source_inventory']=pin('synthetic-runtime-inventory',inventory_obj)
 build_obj=runtime_receipt('build');build_obj['source_inventory_sha256']=declaration['source_inventory']['sha256']
 declaration['build']=pin('synthetic-runtime-build',build_obj)
 image_obj=runtime_receipt('images');image_obj.update(source_inventory_sha256=declaration['source_inventory']['sha256'],build_proof_sha256=declaration['build']['sha256'])
 for row in image_obj['images'].values():row.update(head=runtime_head,tree=runtime_tree,source_inventory_sha256=declaration['source_inventory']['sha256'])
 declaration['images']=pin('synthetic-runtime-images',image_obj)
 review_obj=json.loads(Path(d['source_prereview']['path']).read_bytes())
 review_obj.update(synthetic_only=True,candidate_head=runtime_head,candidate_tree=runtime_tree,source_inventory_sha256=declaration['source_inventory']['sha256'])
 runtime_reviews=[]
 for i,row in enumerate(review_obj['underlying_reviews']):
  value=json.loads(Path(row['path']).read_bytes());value.update(synthetic_only=True,candidate_head=runtime_head,candidate_tree=runtime_tree)
  runtime_reviews.append(pin('synthetic-runtime-review-'+str(i),value))
 review_obj['underlying_reviews']=runtime_reviews
 value=json.loads(Path(review_obj['landing_evidence']['path']).read_bytes());value.update(synthetic_only=True,runtime_head=runtime_head,runtime_tree=runtime_tree,source_inventory_sha256=declaration['source_inventory']['sha256'])
 review_obj['landing_evidence']=pin('synthetic-runtime-landing',value)
 value=json.loads(Path(review_obj['ci_evidence']['path']).read_bytes());value.update(synthetic_only=True,head=runtime_head)
 review_obj['ci_evidence']=pin('synthetic-runtime-ci',value)
 declaration['source_prereview']=pin('synthetic-runtime-acceptance',review_obj)
 declaration.update(output_root=str(root/'trial24-emitted-checkpoint-synthetic-only'),checkpoint_harness={'head':c.CHECKPOINT_HARNESS[0],'tree':c.CHECKPOINT_HARNESS[1],'source_inventory':hi,'source_acceptance':acceptance})
 dr=pin('synthetic-checkpoint-construction-declaration',declaration)
 argv=[sys.executable,'-B',str(R/'instantiate.py'),'--declaration',dr['path'],'--declaration-sha256',dr['sha256']]
 result=subprocess.run(argv,capture_output=True,timeout=30)
 for suffix,data in [('stdout',result.stdout),('stderr',result.stderr),('exit',str(result.returncode).encode()),('argv.json',json.dumps(argv).encode())]:(root/('actual-checkpoint-emission.'+suffix)).write_bytes(data)
 assert result.returncode==0,result.stderr
 emission=json.loads((Path(declaration['output_root'])/'instantiation.json').read_bytes())
 assert emission['source_head']==c.CHECKPOINT_RUNTIME[0] and emission['source_tree']==c.CHECKPOINT_RUNTIME[1]
 for role,row in emission['roles'].items():assert sha(Path(row['path']).read_bytes())==row['sha256'];ast.parse(Path(row['path']).read_bytes())
 emitted=load(emission['roles']['collector']['path'],'checkpoint_actual_emitted_collector')
 assert emitted.CHECKPOINT_ALLOWED is True and emitted.CHECKPOINT_HARNESS==c.CHECKPOINT_HARNESS
 assert emitted.RUN==c.RUN and emitted.W.declaration()==c.W.declaration()
 checks.append('checkpoint_actual_complete_emitted_packet_runtime_harness_split')
 stale_rows=copy.deepcopy(hrows);stale_rows[0]['git_blob']='1'*40
 stale_hi=pin('synthetic-stale-harness-inventory',{'head':c.CHECKPOINT_HARNESS[0],'tree':c.CHECKPOINT_HARNESS[1],'rows':stale_rows,'overlays':{}})
 stale_land=json.loads(raw[land['path']]);stale_land['source_inventory_sha256']=stale_hi['sha256'];sl=pin('synthetic-stale-harness-landing',stale_land)
 stale_acceptance=json.loads(raw[acceptance['path']]);stale_acceptance.update(source_inventory_sha256=stale_hi['sha256'],landing_evidence=sl);sa=pin('synthetic-stale-harness-acceptance',stale_acceptance)
 stale_decl=copy.deepcopy(declaration);stale_decl['output_root']=str(root/'must-not-emit-stale-harness');stale_decl['checkpoint_harness'].update(source_inventory=stale_hi,source_acceptance=sa);sd=pin('synthetic-stale-harness-declaration',stale_decl)
 command=[sys.executable,'-B',str(R/'instantiate.py'),'--declaration',sd['path'],'--declaration-sha256',sd['sha256']]
 result=subprocess.run(command,capture_output=True,timeout=30)
 for suffix,data in [('stdout',result.stdout),('stderr',result.stderr),('exit',str(result.returncode).encode()),('argv.json',json.dumps(command).encode())]:(root/('actual-stale-harness-refusal.'+suffix)).write_bytes(data)
 assert result.returncode!=0 and not Path(stale_decl['output_root']).exists()
 checks.append('checkpoint_actual_instantiator_rejects_repinned_stale_harness_blob_before_output')

 # Appended controls preserve every predecessor label and order.
 checks.append('checkpoint_actual_canonical_observer_two_host_output_admitted')
 for case in ('running','bad_exit','oom','foreign_cid','foreign_image','driver_present','gate_present','gate_symlink'):
  bad('checkpoint_actual_canonical_observer_reject_'+case,lambda case=case:observe(c.HOST,case))
 for case in ('alternate_command','extra_argument','optimized_command','substituted_cid_payload','substituted_image_payload','stdout_hash','stderr_hash','stderr_nonempty','unbounded_timeout','runtime_claim'):
  uu=copy.deepcopy(unused);aa=copy.deepcopy(a);changed=dict(raw);row=uu['raw_evidence'][-1];rr=json.loads(changed[row['path']])
  if case=='alternate_command':rr['argv'][-1]='synthetic-no-execution'
  elif case=='extra_argument':rr['argv'].append('extra')
  elif case=='optimized_command':rr['argv'][-1]=rr['argv'][-1].replace('python3 -c','python3 -O -c',1)
  elif case in ('substituted_cid_payload','substituted_image_payload'):
   values=__import__('shlex').split(rr['argv'][-1]);payload=json.loads(values[-1]);key='cid' if case=='substituted_cid_payload' else 'image';payload['nodes'][0][key]='f'*64;values[-1]=json.dumps(payload);rr['argv'][-1]=__import__('shlex').join(values)
  elif case=='stdout_hash':rr['stdout_sha256']='1'*64
  elif case=='stderr_hash':rr['stderr_sha256']='1'*64
  elif case=='stderr_nonempty':rr['stderr']='unexpected';rr['stderr_sha256']=sha(rr['stderr'].encode())
  elif case=='unbounded_timeout':rr['timeout_seconds']=91
  else:
   obs=json.loads(rr['stdout']);obs['runtime_started']=True;rr['stdout']=json.dumps(obs);rr['stdout_sha256']=sha(rr['stdout'].encode())
  changed[row['path']]=json.dumps(rr).encode();row['sha256']=sha(changed[row['path']]);aa['local_pins'][row['path']]=row['sha256']
  changed[ur['path']]=json.dumps(uu).encode();aa['local_pins'][ur['path']]=sha(changed[ur['path']]);zz=copy.deepcopy(auth);zz['unused_window_proof']['sha256']=aa['local_pins'][ur['path']];changed[ar['path']]=json.dumps(zz).encode()
  bad('checkpoint_actual_unused_raw_command_reject_'+case,lambda aa=aa,changed=changed:c.validate_checkpoint_window(aa,changed,pre))
 for flag in ('-O','-OO'):
  argv=[sys.executable,flag,'-c',c.CHECKPOINT_UNUSED_REMOTE]
  result=subprocess.run(argv,capture_output=True,timeout=10)
  for suffix,data in [('stdout',result.stdout),('stderr',result.stderr),('exit',str(result.returncode).encode()),('argv.json',json.dumps(argv).encode())]:(root/('canonical-observer-'+flag[1:]+'.'+suffix)).write_bytes(data)
  assert result.returncode!=0 and b'ordinary Python required' in result.stderr
  checks.append('checkpoint_actual_canonical_observer_optimized_'+flag+'_refusal_before_inspect')
 (root/'canonical-unused-producer-controls.json').write_text(json.dumps(dict(state='SOURCE_ONLY_STUBBED_DOCKER_NOT_ADMISSION',producer_calls=producer_calls,network_calls=0),indent=2)+'\n')

 # Execute the actual main finally with only the external transport stubbed.
 # Expired diagnostics must not prevent bounded ownership, stops or raw logs.
 for case in ('resource_expired','audit_expired','foreign_driver'):
  cc=load(R/m['roles']['collector']['output']['path'],'checkpoint_finally_'+case)
  folder=root/('actual-finally-'+case);folder.mkdir()
  cc.OUTPUT=folder;cc.serial=0;cc.last_record=None;cc.deadline=None;cc.paths={}
  cc.phase_budget=expired;cc.NODES=[dict(n,name='synthetic-owned-'+n['node']) for n in a['nodes']]
  cc.APPROVED=a;cc.root=cc.ROOT;cc.image=a['query_image'];cc.launch_nonce='b'*32
  driver='e'*64;running={n['cid']:True for n in cc.NODES};running[driver]=True
  commands=[]
  def inspection(cid):
   is_driver=cid==driver
   n=None if is_driver else next(v for v in cc.NODES if v['cid']==cid)
   binds={cc.root+'/node-c/config.json:/config.json:ro',cc.root+'/node-c/credentials:/credentials:ro',cc.root+'/bootstrap-qualify.json:/bootstrap.json:ro',cc.INPUT_ROOT+':/recall:ro',cc.GATE_DIR+':'+cc.MOUNT_GATE+':rw'} if is_driver else cc.expected_binds(n)
   return dict(Id=('f'*64 if case=='foreign_driver' and is_driver else cid),Image=cc.image if is_driver else n['image'],Name='/'+(cc.NAME if is_driver else n['name']),Config={'User':a['driver_uid_gid'],'Labels':{'treedb.fixed-cluster.run':cc.RUN,'treedb.fixed-cluster.invocation':cc.launch_nonce}},HostConfig={'Memory':2147483648,'MemorySwap':2147483648,'NanoCpus':2000000000,'RestartPolicy':{'Name':'no'},'NetworkMode':'host','Binds':sorted(binds)},State={'Running':running[cid],'ExitCode':0,'OOMKilled':False})
  report=dict(Writes=[],Prefixes=[dict(PopulationRows=10002,PopulationSHA256='a'*64)],RequiredAppliedIndex=10,AuditPlan={})
  logs='\n'.join(json.dumps(dict(Event=e,Report=report)) for e in ('planned','result'))+'\n'
  def closure_transport(command,**kwargs):
   args=__import__('shlex').split(command[-1]);commands.append(dict(argv=command,timeout=kwargs['timeout']))
   assert kwargs['timeout']<=45
   if args[:2]==['docker','stop']:running[args[-1]]=False;stdout=args[-1]
   elif args[:2]==['docker','inspect']:stdout=json.dumps([inspection(args[-1])])
   elif args[:2]==['docker','logs']:stdout=logs
   elif args==['python3','-c',cc.GATE_SNAPSHOT,cc.GATE_DIR]:stdout=json.dumps({'tokens':{}})
   elif args==['python3','-c',cc.driver_final_probe(driver)]:stdout=json.dumps([inspection(driver)])
   else:raise AssertionError('unexpected final diagnostic/resource transport: '+repr(args))
   return SimpleNamespace(returncode=0,stdout=stdout.encode(),stderr=b'')
  cc.subprocess=SimpleNamespace(run=closure_transport,TimeoutExpired=subprocess.TimeoutExpired)
  cc.__dict__.update(cid=driver,launch_attempted=True,final=None,stdout='',errors=[],outcomes=None,samples=[],initial_vectors={},workload_passed=False,resource_complete=False,audits_before_stop=False,final_population_passed=False)
  if case=='audit_expired':
   # This represents expiry after the final resource sample, before its audit.
   def sampled_then_expired(*args,**kwargs):cc.phase_budget=expired;return []
   cc.sample_all=sampled_then_expired;cc.validate_mixed_report=lambda *args:{}
   cc.apply_originals=lambda *args:{};cc.audit_originals=lambda *args:[]
   cc.population_identity=lambda *args:{'Rows':10002,'SHA256':'a'*64}
   cc.population_plan=lambda *args:{'Writes':[]}
  body=ast.Module(body=copy.deepcopy(final.finalbody),type_ignores=[])
  exec(compile(ast.fix_missing_locations(body),'actual-collector-finally','exec'),cc.__dict__)
  dispatched=[__import__('shlex').split(v['argv'][-1]) for v in commands]
  assert not any(v[:2]==['docker','exec'] or len(v)>2 and v[2]==cc.SAMPLER for v in dispatched)
  assert all(not running[n['cid']] for n in cc.NODES)
  assert len(json.loads((folder/'voter-stop-receipts.json').read_bytes()))==4
  assert cc.errors and not cc.final_population_passed and not cc.audits_before_stop and not cc.workload_passed
  assert not any('NameError' in error for error in cc.errors),cc.errors
  if case=='foreign_driver':assert running[driver] and not (folder/'stdout.jsonl').exists()
  else:
   assert any('prospective checkpoint phase budget exhausted; no replay' in error for error in cc.errors),cc.errors
   assert not running[driver] and (folder/'stdout.jsonl').read_text()==logs
  (folder/'control-result.json').write_text(json.dumps(dict(state='ACTUAL_FINALLY_TRANSPORT_STUB_NOT_ACCEPTANCE',case=case,commands=commands,errors=cc.errors,all_four_owned_stopped=True,network_calls=0),indent=2)+'\n')
  checks.append('checkpoint_actual_finally_expiry_preserves_closure_'+case)
 c.OUTPUT=root/'actual-transport-boundary';c.OUTPUT.mkdir();c.serial=0;c.deadline=None;c.NODES=a['nodes'];c.phase_budget=expired
 commands=[]
 def bounded_transport(command,**kwargs):commands.append(dict(argv=command,timeout=kwargs['timeout']));return SimpleNamespace(returncode=0,stdout=b'synthetic-only',stderr=b'')
 saved=c.subprocess.run;c.subprocess.run=bounded_transport
 try:
  for label,args,kwargs in [('final-population-node-a',['docker','exec','-i',a['nodes'][0]['cid'],'/server','-mode','diagnostics'],{}),('samples-192.168.0.111',['python3','-c',c.SAMPLER,'[]','[]'],{}),('driver-logs',['docker','exec','-i','a'*64,'/server'],{'closure':True}),('driver-final-inspect',['python3','-c','synthetic-no-execution'],{'closure':True,'probe_target':'a'*64}),('stop-node-a',['docker','stop','--time','30','f'*64],{'closure':True,'host':'mikers@192.168.0.111'})]:
   before=len(commands);bad('checkpoint_actual_phase_boundary_refuses_'+label,lambda label=label,args=args,kwargs=kwargs:c.call(label,args,**kwargs));assert len(commands)==before
  assert not list(c.OUTPUT.iterdir())
  c.phase_budget={'deadline_unix':time.time()+.5}
  c.call('final-population-node-a',['docker','exec','-i',a['nodes'][0]['cid'],'/server','-mode','diagnostics'],timeout=120)
  assert 0<commands[-1]['timeout']<=.5;checks.append('checkpoint_actual_phase_boundary_final_diagnostic_remaining_budget')
  c.phase_budget=None
  c.call('default-dispatch',['synthetic-no-execution'],timeout=120)
  assert commands[-1]['timeout']==120;checks.append('checkpoint_actual_phase_boundary_fresh_campaign_default_unchanged')
 finally:c.subprocess.run=saved;c.phase_budget=None;c.OUTPUT=Path(c.W.output)

checkpoint_controls()
