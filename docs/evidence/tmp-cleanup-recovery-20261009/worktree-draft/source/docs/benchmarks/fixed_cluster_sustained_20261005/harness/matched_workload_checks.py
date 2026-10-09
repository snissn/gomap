"""Pure actual source/consumer controls; synthetic receipts confer no authority."""
if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')

def matched_controls():
 from source_paths import workload_source
 root=fixtures/'matched-source-controls';root.mkdir()
 receipts=[]
 def producer_return(module,function,values):
  # Evaluate the actual final emission expression, without running acquisition.
  source=Path(module.__file__).read_bytes();tree=ast.parse(source)
  node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==function)
  expression=node.body[-1];assert isinstance(expression,ast.Return)
  return eval(compile(ast.Expression(expression.value),str(module.__file__)+'::actual-return','eval'),dict(module.__dict__,**values))
 for filename in ('matched_report.py',m['roles']['guard']['output']['path']):
  folder=root/(Path(filename).stem+'-ordinary-import');folder.mkdir()
  for name in (filename,'source_paths.py','workload_profile.py'):(folder/name).write_bytes((R/name).read_bytes())
  argv=[sys.executable,str(folder/filename),'--help'];proc=subprocess.run(argv,capture_output=True,text=True,timeout=15)
  for suffix,value in [('stdout',proc.stdout),('stderr',proc.stderr),('exit',str(proc.returncode)+'\n'),('argv.json',json.dumps(argv))]:(root/(filename+'.'+suffix)).write_text(value)
  assert proc.returncode==0 and not list(folder.rglob('*.pyc')),(filename,proc.stderr)
  checks.append('matched_actual_ordinary_no_B_cli_no_packet_cache_'+filename)
 for index,concurrency in enumerate(workload_source.MATCHED_ORDER,1):
  campaign='rf4matched5068w%02dc%d'%(index,concurrency)
  profile=workload_source.accepted(campaign,workload_source.Workload(campaign,6,60,5,concurrency).declaration())
  declaration=copy.deepcopy(d);declaration.update(campaign=campaign,workload=profile.declaration(),output_root=str(root/campaign))
  path=root/(campaign+'-declaration.json');path.write_text(json.dumps(declaration))
  argv=[sys.executable,'-B',str(R/'instantiate.py'),'--declaration',str(path),'--declaration-sha256',sha(path.read_bytes())]
  proc=subprocess.run(argv,capture_output=True,text=True,timeout=20)
  for suffix,value in [('stdout',proc.stdout),('stderr',proc.stderr),('exit',str(proc.returncode)+'\n'),('argv.json',json.dumps(argv))]:(root/(campaign+'.'+suffix)).write_text(value)
  assert proc.returncode==0,(campaign,proc.stderr)
  packet=Path(declaration['output_root']);emission=json.loads((packet/'instantiation.json').read_bytes())
  assert emission['workload']==profile.declaration() and emission['runtime_started'] is False
  for role,row in emission['roles'].items():
   assert sha(Path(row['path']).read_bytes())==row['sha256'];ast.parse(Path(row['path']).read_bytes())
  # A fresh child process isolates bound module identities across campaigns.
  program="import sys,importlib.util,json;sys.dont_write_bytecode=True;sys.path.insert(0,"+repr(str(packet))+");from source_paths import W\n"
  for role,row in emission['roles'].items():
   program+="spec=importlib.util.spec_from_file_location("+repr('matched_'+role)+","+repr(row['path'])+");module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)\n"
   if role not in ('shared_read','shared_audit'):program+='assert module.W is W\n'
   if role=='read_audit':program+='ra=module\n'
   if role=='resource':program+='resource=module\n'
  program+="ns=resource.pure_collector({'query_image':'sha256:'+'1'*64,'driver_uid_gid':'1000:1000','driver_sha256':'2'*64},'a'*32);argv=ns['driver_arguments']();assert all(argv[argv.index(flag)+1]==value for flag,value in [('-mixed-originals','6'),('-read-window','60s'),('-mixed-interval','5s'),('-read-concurrency',str(W.concurrency)),('-rpc-timeout','3s'),('-timeout','420s')]);print(json.dumps({'campaign':W.campaign,'profile':W.declaration(),'read_duration':ra.rd.DURATION,'spacing':ra.rd.SPACING,'concurrency':ra.rd.CONCURRENCY}))\n"
  childargv=[sys.executable,'-c',program];child=subprocess.run(childargv,capture_output=True,text=True,timeout=20)
  for suffix,value in [('stdout',child.stdout),('stderr',child.stderr),('exit',str(child.returncode)+'\n'),('argv.json',json.dumps(childargv))]:(root/(campaign+'-import.'+suffix)).write_text(value)
  assert child.returncode==0,(campaign,child.stderr)
  actual=json.loads(child.stdout);assert actual==dict(campaign=campaign,profile=profile.declaration(),read_duration=60000000000,spacing=5000000000,concurrency=concurrency)
  assert not list(packet.rglob('*.pyc'))
  receipts.append(dict(campaign=campaign,concurrency=concurrency,instantiation=emission,actual_import=actual))
  checks.append('matched_actual_complete_source_packet_w%02d_c%d'%(index,concurrency))
  for flag in ('-O','-OO'):
   invalid=copy.deepcopy(declaration);invalid['output_root']=str(root/(campaign+'-optimized-'+flag[1:]))
   entry=root/(campaign+'-'+flag[1:]+'-declaration.json');entry.write_text(json.dumps(invalid))
   argv=[sys.executable,flag,str(R/'instantiate.py'),'--declaration',str(entry),'--declaration-sha256',sha(entry.read_bytes())]
   proc=subprocess.run(argv,capture_output=True,text=True,timeout=15)
   for suffix,value in [('stdout',proc.stdout),('stderr',proc.stderr),('exit',str(proc.returncode)+'\n'),('argv.json',json.dumps(argv))]:(root/(campaign+'-'+flag[1:]+'.'+suffix)).write_text(value)
   assert proc.returncode!=0 and 'ordinary Python required' in proc.stderr and not Path(invalid['output_root']).exists()
   checks.append('matched_actual_optimized_before_output_w%02d_%s'%(index,flag[1:]))
  tampered=root/(campaign+'-substituted-profile');tampered.mkdir()
  for filename in ('source_paths.py','workload_profile.py'):(tampered/filename).write_bytes((packet/filename).read_bytes())
  with (tampered/'workload_profile.py').open('a') as f:f.write('\n# substituted source bytes\n')
  program="import sys;sys.dont_write_bytecode=True;sys.path.insert(0,"+repr(str(tampered))+");import source_paths"
  argv=[sys.executable,'-c',program];proc=subprocess.run(argv,capture_output=True,text=True,timeout=15)
  for suffix,value in [('stdout',proc.stdout),('stderr',proc.stderr),('exit',str(proc.returncode)+'\n'),('argv.json',json.dumps(argv))]:(root/(campaign+'-substituted.'+suffix)).write_text(value)
  assert proc.returncode!=0 and 'authenticated workload source' in proc.stderr and not list(tampered.rglob('*.pyc'))
  checks.append('matched_actual_substituted_profile_refused_w%02d'%index)
 for field,value in [('Originals',48),('Prefixes',49),('DurationSeconds',300),('MinSpacingSeconds',6),('Concurrency',True),('Concurrency',4.0),('OutputBytes',134217729)]:
  invalid=copy.deepcopy(declaration);invalid['workload'][field]=value
  bad('matched_exact_profile_rejects_'+field+'_'+repr(value),lambda invalid=invalid:x.declaration(invalid,m))
 for campaign in ('rf4matched5068w01c4','rf4matched5068w02c1','rf4matched5068w07c4'):
  invalid=copy.deepcopy(declaration);invalid['campaign']=campaign
  bad('matched_campaign_order_rejects_'+campaign,lambda invalid=invalid:x.declaration(invalid,m))
 # The existing full producer-shaped fixture runs the actual adapted verifier.
 old=(ra.rd.DURATION,ra.rd.SPACING,ra.rd.CONCURRENCY,ra.rd.c.QUERY_RUN)
 report_windows=[]
 try:
  for window,concurrency in enumerate(workload_source.MATCHED_ORDER,1):
   campaign='rf4matched5068w%02dc%d'%(window,concurrency)
   ra.rd.c.QUERY_RUN=workload_source.Workload(campaign,6,60,5,concurrency).query_run
   ra.rd.DURATION=60000000000;ra.rd.SPACING=5000000000;ra.rd.CONCURRENCY=concurrency
   ns,run=visibility_fixture(ra.rd,6);p,r=ns['p'],ns['r']
   p['Concurrency']=r['Concurrency']=concurrency
   # The Go default-six producer omits the zero-valued Originals member.
   for report_row in (p,r):
    report_row.pop('Originals',None);report_row['Profile']='changing-top10'
   for a in r['Attempts']:
    a['Worker']=a['Ordinal']%concurrency if a['Phase']=='warmup' else ([1,0,2,3,0,1,3,2,0,1,2,3,1,0,3,2][a['Ordinal']] if concurrency==4 else 0)
    if concurrency==4 and a['Phase']=='measured' and a['Ordinal']<4:
     a['StartNS'],a['EndNS']=[(5000000,6000000),(4000000,7000000),(3000000,4000000),(2000000,3000000)][a['Ordinal']]
     a['Deadline']=ns['utc'](a['StartNS']+ra.rd.RPC)
   measured=[a for a in r['Attempts'] if a['Phase']=='measured']
   for a,claim in zip(measured,r['ReadPrefixes']):
    lo,hi=ra.rd.causal_range(r['Writes'],a['StartNS'],a['EndNS'])
    claim.update(Lower=lo,Upper=hi,Matched=lo,CompatibleMask=sum(1<<i for i in range(lo,hi+1)),RecallAt10=1.0)
   r['SuccessLatency']=ra.rd.c.percent([a['EndNS']-a['StartNS'] for a in measured])
   r['OverlappingSearches']=sum(any(a['StartNS']<w['EndNS'] and a['EndNS']>w['StartNS'] for w in r['Writes']) for a in measured)
   r['CompletedSearchesDuringMutation']=sum(any(a['StartNS']>=w['StartNS'] and a['EndNS']<w['EndNS'] for w in r['Writes']) for a in measured)
   r['RetainedAttemptBytes']=sum(len(ns['encode'](a).encode())+1 for a in r['Attempts'])
   ns['ptext']=ns['encode'](p);r['PlannedEventBytes']=len(('{"Event":"planned","Report":'+ns['ptext']+'}\n').encode())
   proof=run();checks.append('matched_actual_complete_read_consumer_w%02d_c%d'%(window,concurrency))
   folder=root/('actual-consumer-w%02d-c%d'%(window,concurrency));folder.mkdir()
   # Add producer-shaped raw audit members; these synthetic attachments are
   # emission characterization, not an execution of the acquisition verifier.
   r['AuditPlan']={'synthetic_only':True};r['Audits']=[{'synthetic_only':True,'node':role} for role in rs.HOSTS]
   proof=run()
   stdout='{"Event":"planned","Report":'+ns['ptext']+'}\n{"Event":"result","Report":'+ns['encode'](r)+'}\n'
   head,tree='1'*40,'2'*40
   native=dict(state='NATIVE_CANONICAL_PREFIX_ORACLES_GENERATED_PENDING_ROOT_VALIDATION',RunID=ra.rd.c.QUERY_RUN,source_head=head,source_tree=tree)
   promoted=dict(native,state='INDEPENDENT_CANONICAL_PREFIX_ORACLES_VERIFIED')
   native_raw=json.dumps(native).encode();promoted_raw=json.dumps(promoted).encode()
   approved=dict(RunID=campaign,source_head=head,source_tree=tree,driver_sha256=r['Admission']['BinarySHA256'],config_sha256=r['Admission']['ConfigSHA256'],bootstrap_sha256=r['Admission']['BootstrapSHA256'],receipts={'prefix_oracles':'/synthetic/promoted'},local_pins={'/synthetic/promoted':sha(promoted_raw)})
   approved_raw=json.dumps(approved).encode()
   auditpairs=ra.core.pieces(ns['encode'](r),'Audits');planraw=ra.core.field(ns['encode'](r),'AuditPlan')[1]
   attachment=producer_return(ra.au,'verify_payload',dict(planraw=planraw,logical=[('synthetic-chain',0)],auditpairs=auditpairs))
   outer=producer_return(ra,'verify',dict(population={'synthetic_only':True},manifest_sha=sha(approved_raw),stdout_sha=sha(stdout.encode()),native_sha=sha(native_raw),a=approved,HEAD=head,TREE=tree,read=proof,audit=attachment))
   peaks={role:1000+window*10+i for i,role in enumerate(rs.HOSTS)};peaks['client']=2000+window
   hwm={role:value//2 for role,value in peaks.items()}
   resource=producer_return(rs,'verifier',dict(stdout_sha256=sha(stdout.encode()),permission={'synthetic_only':True},gp={'synthetic_only':True},peaks=peaks,hwm=hwm))
   sources={path:dict(path=str(ra.core.source_path(path)),sha256=digest) for proof_row in (outer,resource) for path,digest in proof_row['source_pins'].items()}
   for name,value in [('planned',p),('result',r),('proof',proof)]:(folder/(name+'.json')).write_text(ns['encode'](value))
   pins={}
   for name,value in [('stdout',stdout.encode()),('read_audit',json.dumps(outer).encode()),('resources',json.dumps(resource).encode()),('manifest',approved_raw),('native_oracle',native_raw),('promoted_oracle',promoted_raw),('sources',json.dumps(sources).encode()),('artifact_review',b'{"synthetic_only":true,"decision":"NOT_ACTUAL_ACCEPTANCE"}'),('costs',json.dumps(dict(campaign=campaign,setup_seconds=1,oracle_seconds=2,retained_bytes=len(stdout))).encode())]:
    target=folder/(name+'.raw');target.write_bytes(value);pins[name]=dict(path=str(target),sha256=sha(value))
   report_windows.append(dict(campaign=campaign,status='complete',pins=pins))
   negative=[]
   if window>2:continue
   for label,edit in [('negative_worker',lambda z:z['Attempts'][64].__setitem__('Worker',-1)),('worker_out_of_bound',lambda z:z['Attempts'][64].__setitem__('Worker',concurrency)),('bool_worker',lambda z:z['Attempts'][64].__setitem__('Worker',True)),('float_worker',lambda z:z['Attempts'][64].__setitem__('Worker',0.0)),('missing_ordinal',lambda z:z['Attempts'][65].__setitem__('Ordinal',2)),('duplicate_ordinal',lambda z:z['Attempts'][65].__setitem__('Ordinal',0)),('wrong_query',lambda z:z['Attempts'][64].__setitem__('QueryID','substituted')),('warmup_ownership',lambda z:z['Attempts'][1].__setitem__('Worker',0 if concurrency==4 else 1)),('future_prefix',lambda z:z['ReadPrefixes'][0].__setitem__('Lower',6)),('unknown_outcome',lambda z:z['Attempts'][64].__setitem__('Outcome','unknown'))]:
    z=copy.deepcopy(r);edit(z);bad('matched_c%d_%s'%(concurrency,label),lambda z=z:run(z));negative.append(label)
   if concurrency==4:
    assert measured[0]['StartNS']>measured[1]['StartNS'] and ra.rd.c.stamp(measured[0]['Deadline'])>ra.rd.c.stamp(measured[1]['Deadline'])
    checks.append('matched_c4_dynamic_worker_cross_worker_overlap_nonmonotonic_global_deadlines')
    z=copy.deepcopy(r);z['Attempts'][65]['Worker']=z['Attempts'][64]['Worker'];bad('matched_c4_same_worker_overlap',lambda:run(z))
    z=copy.deepcopy(r);z['Attempts'][68]['Deadline']=z['Attempts'][65]['Deadline'];bad('matched_c4_same_worker_deadline_regression',lambda:run(z))
   receipts.append(dict(concurrency=concurrency,actual_consumer_proof=proof,rejected=negative))
 finally:ra.rd.DURATION,ra.rd.SPACING,ra.rd.CONCURRENCY,ra.rd.c.QUERY_RUN=old
 report=load(R/'matched_report.py','matched_descriptive_report')
 manifest=dict(Version=1,windows=report_windows);summary=report.build(manifest)
 (root/'report-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
 assert [w['concurrency'] for w in summary['windows']]==[1,4,4,1,1,4] and summary['campaign_acceptance'] is False
 assert all(arm['complete_windows']==3 for arm in summary['per_arm'].values())
 checks.append('matched_actual_descriptive_report_six_windows_arm_median_min_max_no_pooled_tail')
 failed=copy.deepcopy(manifest);failed['windows'][1].update(status='failed',pins={});incomplete=copy.deepcopy(failed);incomplete['windows'][2].update(status='incomplete',pins={})
 partial=report.build(incomplete);assert len(partial['windows'])==6 and partial['per_arm']['C4']['complete_windows']==1
 checks.append('matched_report_retains_failed_incomplete_order_without_replacements')
 z=copy.deepcopy(manifest);z['windows'][0],z['windows'][1]=z['windows'][1],z['windows'][0];bad('matched_report_rejects_reordered_arms',lambda:report.build(z))
 z=copy.deepcopy(manifest);z['windows'][0]['pins']['stdout']['sha256']='0'*64;bad('matched_report_rejects_substituted_raw_result',lambda:report.build(z))
 # Existing 659 labels retain their order; append discriminating real-shape joins.
 for w in summary['windows']:
  original=json.loads(Path(w['raw_refs']['resources']['path']).read_bytes())
  assert w['metrics']['memory_peak_bytes']==original['observed_memory_peak_bytes'] and w['metrics']['process_hwm_bytes']==original['observed_process_hwm_bytes']
 checks.append('matched_report_actual_producer_return_shapes_outer_provenance_and_five_role_maps')
 assert summary['per_arm']['C1']['observed_role_bytes']['memory_peak_bytes']['client']==dict(median=2004,minimum=2001,maximum=2005)
 checks.append('matched_report_per_role_arm_summary_without_noncontemporaneous_sum')
 def changed_ref(name,label,edit):
  z=copy.deepcopy(manifest);ref=z['windows'][0]['pins'][name];value=json.loads(Path(ref['path']).read_bytes());value=edit(value)
  def pin(role,value):
   raw=json.dumps(value).encode();target=root/('report-negative-'+label+'-'+role+'.json');target.write_bytes(raw)
   z['windows'][0]['pins'][role]=dict(path=str(target),sha256=sha(raw));return sha(raw)
  digest=pin(name,value)
  if name in ('native_oracle','promoted_oracle','manifest'):
   # Rebind dependent synthetic hashes; the changed identity/admission field,
   # rather than an earlier raw-hash failure, must cause these refusals.
   outer=json.loads(Path(z['windows'][0]['pins']['read_audit']['path']).read_bytes())
   if name=='native_oracle':outer['native_oracle_sha256']=digest
   elif name=='promoted_oracle':
    outer['promoted_oracle_sha256']=digest
    approved=json.loads(Path(z['windows'][0]['pins']['manifest']['path']).read_bytes())
    approved['local_pins'][approved['receipts']['prefix_oracles']]=digest
    outer['manifest_sha256']=pin('manifest',approved)
   else:outer['manifest_sha256']=digest
   pin('read_audit',outer)
  return z
 negatives=[('naked_read','read_audit',lambda v:v['read']),('lost_provenance','read_audit',lambda v:{k:x for k,x in v.items() if k!='source_pins'}),('outer_stdout','read_audit',lambda v:dict(v,stdout_sha256='0'*64)),('outer_manifest','read_audit',lambda v:dict(v,manifest_sha256='0'*64)),('outer_native','read_audit',lambda v:dict(v,native_oracle_sha256='0'*64)),('outer_promoted','read_audit',lambda v:dict(v,promoted_oracle_sha256='0'*64)),('outer_source','read_audit',lambda v:dict(v,source_head='3'*40)),('read_result','read_audit',lambda v:dict(v,read=dict(v['read'],raw_result_report_sha256='0'*64))),('read_planned','read_audit',lambda v:dict(v,read=dict(v['read'],raw_planned_report_sha256='0'*64))),('audit_plan','read_audit',lambda v:dict(v,audit=dict(v['audit'],plan_sha256='0'*64))),('audit_attachments','read_audit',lambda v:dict(v,audit=dict(v['audit'],audits_sha256=['0'*64]*4))),('resource_stdout','resources',lambda v:dict(v,stdout_sha256='0'*64)),('source_mapping','sources',lambda v:{k:x for i,(k,x) in enumerate(v.items()) if i})]
 for key in ('observed_memory_peak_bytes','observed_process_hwm_bytes'):
  for label,value in [('scalar',123),('missing_role',{'client':1}),('extra_role',dict.fromkeys((*report.ROLES,'foreign'),1)),('bool',dict.fromkeys(report.ROLES,True)),('float',dict.fromkeys(report.ROLES,1.0)),('negative',dict.fromkeys(report.ROLES,-1))]:
   negatives.append((key+'_'+label,'resources',lambda v,key=key,value=value:dict(v,**{key:value})))
 for label,name,edit in negatives:
  z=changed_ref(name,label,edit);bad('matched_report_actual_shape_rejects_'+label,lambda z=z:report.build(z))
 for label,name,edit in [('resource_lost_sources','resources',lambda v:dict(v,source_pins={})),('conflicting_sources','resources',lambda v:dict(v,source_pins={k:'0'*64 for k in v['source_pins']})),('extra_source_mapping','sources',lambda v:dict(v,**{'/synthetic/extra-source':next(iter(v.values()))})),('substituted_source_bytes','sources',lambda v:{k:dict(ref,sha256='0'*64) for k,ref in v.items()}),('native_source_identity','native_oracle',lambda v:dict(v,source_tree='3'*40)),('promoted_source_identity','promoted_oracle',lambda v:dict(v,source_head='3'*40)),('manifest_admission','manifest',lambda v:dict(v,driver_sha256='0'*64))]:
  z=changed_ref(name,label,edit)
  reason={'native_source_identity':'oracle source/query identity','promoted_source_identity':'oracle source/query identity','manifest_admission':'raw admission/manifest BinarySHA256'}.get(label)
  if reason is None:bad('matched_report_actual_shape_rejects_'+label,lambda z=z:report.build(z))
  else:
   try:report.build(z)
   except ValueError as error:assert str(error)==reason,(label,str(error));checks.append('matched_report_actual_shape_rejects_'+label)
   else:raise AssertionError('accepted '+label)
 staged=root/'report-staged-sources';staged.mkdir();mapping={}
 original=json.loads(Path(manifest['windows'][0]['pins']['sources']['path']).read_bytes())
 for i,(identity,ref) in enumerate(original.items()):
  target=staged/('%02d-source.py'%i);target.write_bytes(Path(ref['path']).read_bytes());mapping[identity]=dict(path=str(target),sha256=ref['sha256'])
 z=changed_ref('sources','byte_identical_staged_sources',lambda v:mapping)
 assert report.build(z)['windows'][0]['metrics']==summary['windows'][0]['metrics']
 checks.append('matched_report_checked_byte_identical_staged_source_mapping')
 (root/'descriptive-report.json').write_text(json.dumps(summary,indent=2)+'\n')
 (root/'integration.json').write_text(json.dumps({'state':'SYNTHETIC_SOURCE_AND_ACTUAL_READ_CONSUMER_ONLY','receipts':receipts,'runtime_started':False,'network_calls':0,'limitations':['Synthetic native-table rows exercise accounting, not actual ranking or campaign acceptance.','Real collection and independent final source/artifact acceptance are outstanding.']},indent=2)+'\n')

matched_controls()

# Permission observation must use the read-only driver's supported duration,
# independently of the immutable 300s mixed checkpoint or matched C1/C4 arm.
permission_root=fixtures/'permission-duration-controls';permission_root.mkdir()
for label,packet in [('original_c1',R),('matched_c1',fixtures/'matched-source-controls/rf4matched5068w01c1'),('matched_c4',fixtures/'matched-source-controls/rf4matched5068w02c4')]:
 program="""import sys,pathlib,importlib.util,json,copy
sys.dont_write_bytecode=True
root=pathlib.Path(sys.argv[1]);sys.path.insert(0,str(root))
def load(filename,name):
 spec=importlib.util.spec_from_file_location(name,root/filename);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
c=load('gomap-4997-4998-trial24mixedchangingc1-collector-root-v1.py','actual_permission_collector')
p=load('gomap-trial24-permission-stage-prepare-root-v1.py','actual_permission_producer')
a={'query_image':'sha256:'+'1'*64,'driver_uid_gid':'1000:1000','driver_sha256':'2'*64};nonce='a'*32
c.APPROVED=a;c.root=c.ROOT;c.image=a['query_image'];c.launch_nonce=nonce
mixed=c.driver_arguments();argv=p.isolated_arguments(c,a,nonce)
assert argv[argv.index('-mode')+1]=='read-window' and argv[argv.index('-read-window')+1]=='60s'
assert '--network=none' in argv and '--network=host' not in argv
assert all(argv[argv.index(flag)+1]==value for flag,value in [('-read-concurrency',str(c.W.concurrency)),('-timeout','420s'),('-rpc-timeout','3s'),('-read-warmup','64'),('-read-max-attempts','65536'),('-read-output-bytes','134217728')])
checks=['supported60s_isolated_actual_argv_and_preserved_caps']
assert c.driver_arguments()==mixed and mixed[mixed.index('-read-window')+1]==str(c.W.duration)+'s'
checks.append('mixed_actual_argv_unchanged')
for label,value in [('unsupported300s','300s'),('unsupported61s','61s'),('substituted59s','59s')]:
 forged=copy.deepcopy(argv);forged[forged.index('-read-window')+1]=value
 try:p.validate_command(c,a,nonce,forged)
 except ValueError:checks.append('reject_'+label)
 else:raise AssertionError('accepted '+label)
for label,forged in [('missing_duration',argv[:argv.index('-read-window')]+argv[argv.index('-read-window')+2:]),('extra_duration',argv+['-read-window','60s']),('restored_mixed_mode',copy.deepcopy(argv))]:
 if label=='restored_mixed_mode':forged[forged.index('-mode')+1]='mixed-window'
 try:p.validate_command(c,a,nonce,forged)
 except ValueError:checks.append('reject_'+label)
 else:raise AssertionError('accepted '+label)
print(json.dumps({'checks':checks,'mixed_argv':mixed,'permission_argv':argv,'runtime_started':False,'network_calls':0}))
"""
 argv=[sys.executable,'-B','-c',program,str(packet)]
 proc=subprocess.run(argv,capture_output=True,text=True,timeout=20)
 for suffix,value in [('stdout',proc.stdout),('stderr',proc.stderr),('exit',str(proc.returncode)+'\n'),('argv.json',json.dumps(argv))]:(permission_root/(label+'.'+suffix)).write_text(value)
 assert proc.returncode==0,(label,proc.stderr)
 result=json.loads(proc.stdout);assert len(result['checks'])==8 and not result['runtime_started'] and result['network_calls']==0
 checks.extend('permission_'+label+'_'+name for name in result['checks'])

# Append new schema controls after every predecessor label. Exercise the actual
# reporter CLI and complete pinned producer-shaped packet, not a field-only stub.
report_root=fixtures/'matched-source-controls'
report_schema=report_root/'raw-schema-controls';report_schema.mkdir()
report_manifest=json.loads((report_root/'report-manifest.json').read_bytes())
def report_cli(label,manifest,expected,reason=None,reporter=None):
 entry=report_schema/(label+'-windows.json');entry.write_text(json.dumps(manifest))
 output=report_schema/(label+'-output.json')
 argv=[sys.executable,'-B',str(reporter or R/'matched_report.py'),'--windows',str(entry),'--windows-sha256',sha(entry.read_bytes()),'--out',str(output)]
 proc=subprocess.run(argv,capture_output=True,timeout=20)
 for suffix,value in [('stdout',proc.stdout),('stderr',proc.stderr),('exit',(str(proc.returncode)+'\n').encode()),('argv.json',json.dumps(argv).encode())]:(report_schema/(label+'.'+suffix)).write_bytes(value)
 assert (proc.returncode==0)==expected,(label,proc.stderr)
 assert output.exists()==expected,(label,'refusal must leave output absent')
 if reason is not None:assert proc.stderr.decode().splitlines()[-1]=='ValueError: '+reason,(label,'wrong refusal',proc.stderr)
 checks.append('matched_report_actual_CLI_schema_'+label)
 return json.loads(output.read_bytes()) if expected else None
full=report_cli('omitted_default_six_complete',report_manifest,True)
assert all(w['metrics']['originals']==6 and w['metrics']['prefixes']==7 for w in full['windows'])
partial_manifest=copy.deepcopy(report_manifest)
for row in partial_manifest['windows'][3:]:row.update(status='incomplete',pins={})
partial=report_cli('omitted_default_six_partial',partial_manifest,True)
assert [w['status'] for w in partial['windows']]==['complete']*3+['incomplete']*3
assert partial['per_arm']['C1']['complete_windows']==1 and partial['per_arm']['C4']['complete_windows']==2
schema_negatives=[('originals_bool',lambda r:r.__setitem__('Originals',True)),('originals_float',lambda r:r.__setitem__('Originals',6.0)),('originals_stale48',lambda r:r.__setitem__('Originals',48)),('originals_null',lambda r:r.__setitem__('Originals',None)),('profile',lambda r:r.__setitem__('Profile','invariant')),('kind',lambda r:r.__setitem__('Kind','fixed_cluster_read_window_v1')),('writes_missing',lambda r:r['Writes'].pop()),('writes_duplicate',lambda r:r['Writes'][1].__setitem__('Ordinal',0)),('writes_bool',lambda r:r['Writes'][0].__setitem__('Ordinal',False)),('prefix_missing',lambda r:r['Prefixes'].pop()),('prefix_duplicate',lambda r:r['Prefixes'][1].__setitem__('Prefix',0)),('prefix_float',lambda r:r['Prefixes'][0].__setitem__('Prefix',0.0))]
for field in ('Version','Concurrency','WarmupPlanned','MaxAttempts','OutputBytes','RequestedDuration','PaceInterval'):
 schema_negatives.extend([(field+'_bool',lambda r,field=field:r.__setitem__(field,True)),(field+'_float',lambda r,field=field:r.__setitem__(field,float(r[field]))),(field+'_wrong',lambda r,field=field:r.__setitem__(field,r[field]+1))])
report_gate=load(R/'matched_report.py','schema_control_reporter')
def synthetic_pin(manifest,role,label,raw):
 target=report_schema/(label+'-'+role+'.raw');target.write_bytes(raw)
 manifest['windows'][0]['pins'][role]=dict(path=str(target),sha256=sha(raw))
def rebind_synthetic_reports(manifest,events,label):
 # Only synthetic fixtures change. Re-authenticate every affected raw join so
 # unrelated stale provenance cannot conceal a missing schema guard.
 lines=[json.dumps(row,separators=(',',':')).encode() for row in events]
 raw=b''.join(line+b'\n' for line in lines)
 planned_raw=report_gate.field_raw(lines[0],'Report');result_raw=report_gate.field_raw(lines[1],'Report')
 outer=json.loads(Path(manifest['windows'][0]['pins']['read_audit']['path']).read_bytes())
 resource=json.loads(Path(manifest['windows'][0]['pins']['resources']['path']).read_bytes())
 outer['stdout_sha256']=resource['stdout_sha256']=sha(raw)
 outer['read'].update(raw_planned_report_sha256=sha(planned_raw),raw_result_report_sha256=sha(result_raw))
 outer['audit']['plan_sha256']=sha(report_gate.field_raw(result_raw,'AuditPlan'))
 outer['audit']['audits_sha256']=[sha(text.encode()) for _,text in ra.core.pieces(result_raw.decode(),'Audits')]
 for role,value in [('stdout',raw),('read_audit',json.dumps(outer).encode()),('resources',json.dumps(resource).encode())]:synthetic_pin(manifest,role,label,value)
guard_reasons={'originals':'matched declared original count','typed':'exact typed matched raw report profile','identity':'matched kind/profile/query identity','writes':'contiguous matched Writes','prefix':'contiguous matched Prefixes'}
def schema_guard(label):
 if label.startswith('originals_'):return 'originals'
 if label in ('profile','kind'):return 'identity'
 if label.startswith('writes_'):return 'writes'
 if label.startswith('prefix_'):return 'prefix'
 return 'typed'
mutants={}
for guard,reason in guard_reasons.items():
 folder=report_schema/('bypassed-'+guard);folder.mkdir()
 tree=ast.parse((R/'matched_report.py').read_bytes())
 original_tree=ast.dump(tree,include_attributes=False)
 function=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='matched_profile')
 calls=[n for n in ast.walk(function) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='need']
 if guard in ('writes','prefix'):
  selected=[n for n in calls if isinstance(n.args[-1],ast.BinOp) and isinstance(n.args[-1].left,ast.Constant) and n.args[-1].left.value=='contiguous matched ']
  assert len(selected)==1
  predicate=selected[0].args[0]
  selected[0].args[0]=ast.BoolOp(op=ast.Or(),values=[ast.Compare(left=ast.Name(id='key',ctx=ast.Load()),ops=[ast.Eq()],comparators=[ast.Constant('Writes' if guard=='writes' else 'Prefixes')]),selected[0].args[0]])
 else:
  selected=[n for n in calls if isinstance(n.args[-1],ast.Constant) and n.args[-1].value==reason]
  assert len(selected)==1;predicate=selected[0].args[0];selected[0].args[0]=ast.Constant(True)
 mutant_predicate=selected[0].args[0];selected[0].args[0]=predicate
 assert ast.dump(tree,include_attributes=False)==original_tree
 selected[0].args[0]=mutant_predicate
 ast.fix_missing_locations(tree)
 target=folder/'matched_report.py';target.write_text(ast.unparse(tree)+'\n')
 for name in ('source_paths.py','workload_profile.py'):(folder/name).write_bytes((R/name).read_bytes())
 mutants[guard]=target
for event in (0,1):
 for label,edit in schema_negatives:
  manifest=copy.deepcopy(report_manifest);ref=manifest['windows'][0]['pins']['stdout']
  events=[json.loads(line) for line in Path(ref['path']).read_bytes().splitlines()]
  edit(events[event]['Report']);name='event%d_%s'%(event,label)
  rebind_synthetic_reports(manifest,events,name)
  guard=schema_guard(label)
  report_cli(name,manifest,False,reason=guard_reasons[guard])
  # Removing only the corresponding predicate now admits the same pinned
  # input. Thus its normal refusal control would fail, rather than passing on
  # a different raw/provenance error. Mutants confer no acceptance authority.
  report_cli('bypassed_'+name,manifest,True,reporter=mutants[guard])
