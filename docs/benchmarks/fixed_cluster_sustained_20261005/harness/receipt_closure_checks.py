"""Additional source-only receipt joins, executed after the retained 291 controls.
All acceptance-shaped bytes below are owned synthetic fixtures, never authority.
"""
if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import contextlib,io,types
assert len(checks)==291
head,tree=d['source_head'],d['source_tree']
closure_receipts=[]
# Declaration inspects the real reference validator, including scoped blob
# applicability to a different reviewed head/tree, without whole-tree equality.
minimal=copy.deepcopy(d);minimal['source_prereview']=fixture('closure-minimal-review',{'decision':'ACCEPT','candidate_head':head,'candidate_tree':tree})
bad('closure_declaration_rejects_minimal_decision_only',lambda:x.declaration(minimal,m))
for key in ('underlying_reviews','landing_evidence','ci_evidence'):
 obj=copy.deepcopy(objects['source_prereview']);obj.pop(key)
 row=copy.deepcopy(d);row['source_prereview']=fixture('closure-missing-'+key,obj)
 bad('closure_declaration_missing_'+key,lambda row=row:x.declaration(row,m))
obj=copy.deepcopy(objects['source_prereview']);obj['underlying_reviews']=[reviews[0],reviews[0]]
row=copy.deepcopy(d);row['source_prereview']=fixture('closure-duplicate-reviews',obj)
bad('closure_declaration_duplicate_independent_receipt',lambda:x.declaration(row,m))
prior=fixture('closure-prior-scoped-review',{'synthetic_only':True,'decision':'ACCEPT','candidate_head':'a'*40,'candidate_tree':'b'*40,'findings':[],'reviewer':'synthetic-prior-scoped'})
obj=copy.deepcopy(objects['source_prereview']);obj['underlying_reviews']=[reviews[0],prior]
row=copy.deepcopy(d);row['source_prereview']=fixture('closure-unjoined-prior-review',obj)
bad('closure_declaration_prior_review_requires_scoped_equality',lambda:x.declaration(row,m))
equality={'synthetic_only':True,'state':'REVIEWED_SOURCE_BLOBS_EQUAL_FINAL_SOURCE','final_head':head,'final_tree':tree,'reviewed_blobs':{'synthetic.go':'3'*40},'reviews':{prior['sha256']:{'candidate_head':'a'*40,'candidate_tree':'b'*40}}}
obj['review_source_equality']=fixture('closure-scoped-equality',equality)
row['source_prereview']=fixture('closure-joined-prior-review',obj)
x.declaration(row,m);checks.append('closure_declaration_scoped_blob_equality_positive')
equality['reviewed_blobs']['synthetic.go']='c'*40
obj['review_source_equality']=fixture('closure-wrong-scoped-equality',equality);row['source_prereview']=fixture('closure-wrong-applicability-review',obj)
bad('closure_declaration_scoped_equality_wrong_final_blob',lambda:x.declaration(row,m))

head_only=fixture('closure-head-only-review',{'synthetic_only':True,'disposition':'ACCEPT','head':'a'*40,'reviewer':'synthetic-head-only','material_findings':[],'findings':[{'severity':'LOW','blocking':False,'finding':'Synthetic nonblocking note'}]})
obj=copy.deepcopy(objects['source_prereview']);obj['underlying_reviews']=[reviews[0],head_only]
row=copy.deepcopy(d);row['source_prereview']=fixture('closure-head-only-unjoined',obj)
bad('closure_declaration_head_only_review_requires_tree_applicability_proof',lambda:x.declaration(row,m))
equality={'synthetic_only':True,'state':'REVIEWED_SOURCE_BLOBS_EQUAL_FINAL_SOURCE','final_head':head,'final_tree':tree,'reviewed_blobs':{'synthetic.go':'3'*40},'reviews':{head_only['sha256']:{'candidate_head':'a'*40,'candidate_tree':'b'*40}}}
obj['review_source_equality']=fixture('closure-head-only-scope-equality',equality)
row['source_prereview']=fixture('closure-head-only-joined',obj)
x.declaration(row,m);checks.append('closure_declaration_existing_disposition_head_nonblocking_shape_positive')
equality['reviews'][head_only['sha256']]['candidate_head']='c'*40
obj['review_source_equality']=fixture('closure-head-only-wrong-candidate',equality);row['source_prereview']=fixture('closure-head-only-wrong-join',obj)
bad('closure_declaration_head_only_wrong_original_candidate',lambda:x.declaration(row,m))
material=fixture('closure-material-review',{'synthetic_only':True,'disposition':'ACCEPT','head':head,'tree':tree,'material_findings':[{'severity':'P1'}],'findings':[]})
obj=copy.deepcopy(objects['source_prereview']);obj['underlying_reviews']=[reviews[0],material];row['source_prereview']=fixture('closure-unresolved-material',obj)
bad('closure_declaration_unresolved_material_review',lambda:x.declaration(row,m))

bp=load(portable_sources['bootstrap_plan'],'trial24_actual_bootstrap_closure')
tuple_paths={key:d[field]['path'] for key,field in (('build','build'),('images','images'),('source_acceptance','source_prereview'))}
assert bp.frozen_product(tuple_paths)==normalized
checks.append('closure_original_receipts_derive_strict_normalized_tuple')
copy_paths={}
for key,path in tuple_paths.items():
 target=fixtures/('trial24-staged-original-'+key+'.json');target.write_bytes(Path(path).read_bytes());copy_paths[key]=str(target)
assert bp.frozen_product(copy_paths)==normalized
checks.append('closure_explicit_same_byte_staged_receipt_mapping_positive')
assert sha(normalized_raw)!=d['build']['sha256']
bp.normalized_product(normalized_raw,Path(product_copy).read_bytes(),normalized)
checks.append('closure_distinct_normalized_raw_SHA_positive')

# Actual bootstrap context and plan writer, with only dataset/TLS dependencies
# controlled. Real source hashes, product receipts, config assertions, plan
# serialization and output guards run; no certificate or dataset qualification.
hosts={'node-a':'192.168.0.111','node-b':'192.168.0.111','node-c':'192.168.0.185','node-d':'192.168.0.185'}
virtual={}
configs={}
for i,(node,host) in enumerate(hosts.items()):
 path='/tmp/'+bp.PREFIX+'-synthetic-config-'+node+'.json'
 cfg={'NodeID':node,'DataRoot':'/home/mikers/gomap-4250-twohost-'+bp.CAMPAIGN+'/'+node+'/data','RaftRoot':'/home/mikers/gomap-4250-twohost-'+bp.CAMPAIGN+'/'+node+'/raft','ListenAddress':host+':'+str(19101+i),'RaftListen':{'meta':host+':'+str(19201+i),'group-a':host+':'+str(19301+i)},'VectorInitialization':{'IndexDefinition':{'name':'embedding_graph','field':'embedding','metric':'cosine','dimensions':128,'m':16,'ef_construction':128,'ef_search':128,'strategy':'column_graph'},'MaxSourceRows':10003,'PublicAddresses':{n:'127.0.0.1:'+str(19401+j) for j,n in enumerate(hosts)},'ShardAddresses':{'group-a':{n:h+':'+str(19501+j) for j,(n,h) in enumerate(hosts.items())}}},'Nodes':[{'ID':n,'Address':h+':'+str(19101+j)} for j,(n,h) in enumerate(hosts.items())],'Catalog':{'Peers':[{'ID':n,'Address':h+':'+str(19201+j)} for j,(n,h) in enumerate(hosts.items())]},'Groups':[{'Peers':[{'ID':n,'Address':h+':'+str(19301+j)} for j,(n,h) in enumerate(hosts.items())]}]}
 raw=json.dumps(cfg).encode();virtual[path]=raw;configs[node]={'path':path,'sha256':sha(raw)}
nb='Oct  1 00:00:00 2026 GMT';na='Oct  8 00:00:00 2026 GMT'
credentials={'synthetic_only':True,'cluster':'synthetic-cluster','days':7,'configs':[{'node':n,'sha256':v['sha256']} for n,v in configs.items()],'public_certificates':[{'path':'synthetic-cert','sha256':'a'*64,'metadata':{'notBefore':nb,'notAfter':na}}]}
credential_pin=fixture('closure-synthetic-credential-provenance',credentials)
volume=fixtures/bp.PREFIX;volume.mkdir()
dataset_path=fixtures/'trial24-controlled-dataset';dataset_path.mkdir()
dataset={'Rows':10000,'Dimensions':128,'SourceRows':10003,'ManifestSHA256':'a'*64,'VectorsSHA256':'b'*64}
context_pins={'synthetic_only':True,'RootAcceptedFinalSourceAndInputs':True,'head':head,'tree':tree,'source_worktree':d['source_root'],'dataset':str(dataset_path),'dataset_manifest_sha256':dataset['ManifestSHA256'],'dataset_vectors_sha256':dataset['VectorsSHA256'],'configs':configs,'credential_provenance':credential_pin['path'],'credential_provenance_sha256':credential_pin['sha256'],'credential_minimum_remaining_seconds':86400,'credential_not_before_unix':bp.ssl.cert_time_to_seconds(nb),'credential_not_after_unix':bp.ssl.cert_time_to_seconds(na),'artifact_volume':str(volume),'qualification_source_sha256':'b'*64}
for key,field in (('build','build'),('images','images'),('source_acceptance','source_prereview')):
 context_pins[key]=d[field]['path'];context_pins[key+'_sha256']=d[field]['sha256']
context_pin=fixture('closure-bootstrap-context',context_pins)
original_spec=importlib.util.spec_from_file_location
def controlled_read(original):
 def read(path,digest=None):
  if str(path) not in virtual:return original(path,digest)
  raw=virtual[str(path)];assert digest is None or sha(raw)==digest;return raw
 return read
bp.read=controlled_read(bp.read)
def controlled_spec(name,path,*args,**kwargs):
 spec=original_spec(name,path,*args,**kwargs)
 if name not in ('fixed_cluster','fresh_credential_guard','trial24_plan'):return spec
 loader=spec.loader
 class ControlledLoader:
  def create_module(self,spec):return loader.create_module(spec)
  def exec_module(self,module):
   loader.exec_module(module)
   if name=='trial24_plan':module.read=controlled_read(module.read)
   elif name=='fresh_credential_guard':module.validate_configs=lambda *a,**k:{'cluster_id':'synthetic-cluster','public_credentials':[]}
   else:
    module.admit_dataset=lambda path:copy.deepcopy(dataset)
    module.read_json=lambda path:{'queries':16}
    module.plan=lambda manifest,campaign,data:[{'node':n,'host':h,'image':manifest['image'][h],'root':'/home/mikers/gomap-4250-twohost-'+campaign+'/'+n,'name':'treedb-4250-'+campaign+'-'+n,'config':json.loads(virtual[configs[n]['path']])} for n,h in hosts.items()]
 spec.loader=ControlledLoader();return spec
original_argv=sys.argv
original_popen=subprocess.Popen
original_read_bytes=Path.read_bytes
try:
 Path.read_bytes=lambda path:virtual[str(path)] if str(path) in virtual else original_read_bytes(path)
 importlib.util.spec_from_file_location=controlled_spec
 subprocess.Popen=lambda *a,**k:(_ for _ in ()).throw(AssertionError('external calls prohibited by closure fixtures'))
 a,launcher,manifest_actual,ds,config_shas=bp.context(context_pin['path'])
 assert manifest_actual['image']==host_images and manifest_actual['binary_sha256']==normalized['server_sha256']
 checks.append('closure_actual_bootstrap_context_exact_declared_tuple_positive')
 for key in ('images','source_acceptance'):
  value=copy.deepcopy(objects['images' if key=='images' else 'source_prereview'])
  if key=='images':value['images']['111']['image']='sha256:'+'e'*64
  else:value['synthetic_alternate_bytes']=True
  changed_pin=fixture('closure-context-substitution-'+key,value)
  alternate=copy.deepcopy(context_pins);alternate[key]=changed_pin['path'];alternate[key+'_sha256']=changed_pin['sha256']
  pins=fixture('closure-context-repinned-'+key,alternate)
  bad('closure_actual_bootstrap_context_rejects_repinned_'+key,lambda pins=pins:bp.context(pins['path']))
 plan_root=fixtures/(bp.PREFIX+'-bootstrap-plan-root-v-closure')
 sys.argv=['source-only-plan','--pins',context_pin['path'],'--out',str(plan_root)]
 stdout=io.StringIO()
 with contextlib.redirect_stdout(stdout):bp.main()
 (fixtures/'closure-actual-bootstrap-plan.stdout').write_text(stdout.getvalue())
 prep=json.loads((plan_root/'plan-preparation.json').read_bytes())
 assert prep['images_receipt_sha256']==d['images']['sha256'] and prep['source_review_sha256']==d['source_prereview']['sha256'] and prep['product']==normalized
 checks.append('closure_actual_bootstrap_plan_retains_complete_immutable_tuple')
 pf=load(portable_sources['bootstrap_preflight'],'trial24_actual_preflight_closure')
 pf_main=next(n for n in ast.parse(Path(portable_sources['bootstrap_preflight']).read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name=='main')
 boundary=next(i for i,n in enumerate(pf_main.body) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='wt' for t in n.targets))
 sys.argv=['source-only-preflight','--pins',context_pin['path'],'--plan',str(plan_root),'--out',str(fixtures/(bp.PREFIX+'-bootstrap-precollection-root-v-closure'))]
 exec(compile(ast.Module(body=pf_main.body[:boundary],type_ignores=[]),'actual-preflight-before-Git-boundary','exec'),pf.__dict__)
 checks.append('closure_actual_preflight_prep_tuple_before_Git_positive')
 pf_proof=next(n for n in pf_main.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='proof' for t in n.targets))
 pf.__dict__.update(receipts={'192.168.0.111':{'synthetic_only':True},'192.168.0.185':{'synthetic_only':True}},volume=volume)
 exec(compile(ast.Module(body=[pf_proof],type_ignores=[]),'actual-preflight-proof-producer-assignment','exec'),pf.__dict__)
 actual_proof=pf.proof;actual_proof['synthetic_only']=True
 life=load(portable_sources['bootstrap_lifecycle'],'trial24_actual_lifecycle_closure')
 lm=next(n for n in ast.parse(Path(portable_sources['bootstrap_lifecycle']).read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name=='main')
 mkdir=next(i for i,n in enumerate(lm.body) if isinstance(n,ast.Expr) and isinstance(n.value,ast.Call) and isinstance(n.value.func,ast.Attribute) and isinstance(n.value.func.value,ast.Name) and n.value.func.value.id=='ROOT' and n.value.func.attr=='mkdir')
 life_prefix=ast.Module(body=lm.body[:mkdir+1],type_ignores=[])
 def lifecycle_case(label,frozen_obj,proof_obj,positive):
  pr=fixtures/(bp.PREFIX+'-bootstrap-plan-root-v-'+label);pr.mkdir()
  (pr/'manifest.json').write_bytes((plan_root/'manifest.json').read_bytes());(pr/'plan-preparation.json').write_bytes(json.dumps(frozen_obj).encode())
  proof_obj=copy.deepcopy(proof_obj);proof_obj['plan_preparation_sha256']=sha((pr/'plan-preparation.json').read_bytes())
  pre=fixtures/(bp.PREFIX+'-bootstrap-precollection-root-v-'+label);pre.mkdir();raw=json.dumps(proof_obj).encode();(pre/'proof.json').write_bytes(raw)
  target=fixtures/(bp.PREFIX+'-bootstrap-lifecycle-root-v-'+label)
  sys.argv=['source-only-lifecycle','--pins',context_pin['path'],'--plan',str(pr),'--preflight',str(pre),'--preflight-sha256',sha(raw),'--out',str(target)]
  denied=False
  try:exec(compile(life_prefix,'actual-lifecycle-admission-through-first-mkdir','exec'),life.__dict__)
  except (AssertionError,ValueError,KeyError):denied=True
  assert denied!=positive and target.exists()==positive,(label,'lifecycle tuple gate/mutation')
  closure_receipts.append({'producer':'actual_lifecycle_prefix','case':label,'accepted_source_fixture':positive,'output_created':target.exists(),'full_lifecycle_executed':False})
  checks.append('closure_actual_lifecycle_'+label+'_before_launcher')
 lifecycle_case('positive',prep,actual_proof,True)
 for field in ('images_receipt_sha256','source_review_sha256','source_inventory_sha256','driver_sha256'):
  q=copy.deepcopy(actual_proof);q[field]='e'*64;lifecycle_case('proof_'+field,prep,q,False)
 for field in ('images_receipt_sha256','source_review_sha256'):
  q=copy.deepcopy(actual_proof);z=copy.deepcopy(prep);q[field]=z[field]='e'*64
  lifecycle_case('repinned_prep_'+field,z,q,False)
finally:
 importlib.util.spec_from_file_location=original_spec;subprocess.Popen=original_popen;sys.argv=original_argv;Path.read_bytes=original_read_bytes

# Actual freezer function closes proof/prep metadata, staged locators and images
# even when intermediate raw hashes have all been recomputed consistently.
for field in ('images_receipt_sha256','source_review_sha256','driver_sha256','source_inventory_sha256'):
 z=copy.deepcopy(proof);z[field]='e'*64;rows=list(base);rows[3]=encoded(z)
 bad('closure_actual_freezer_proof_'+field,lambda rows=rows:freezer.bootstrap_provenance(*rows))
for field in ('images_receipt_sha256','source_review_sha256'):
 z=copy.deepcopy(frozen);q=copy.deepcopy(proof);z[field]=q[field]='e'*64
 q['plan_preparation_sha256']=sha(encoded(z));rows=list(base);rows[1]=encoded(z);rows[3]=encoded(q)
 bad('closure_actual_freezer_repinned_prep_'+field,lambda rows=rows:freezer.bootstrap_provenance(*rows))
for index,key in ((5,'images'),(6,'source_prereview')):
 z=copy.deepcopy(objects[key]);z['synthetic_changed_bytes']=True;rows=list(base);rows[index]=encoded(z)
 bad('closure_actual_freezer_wrong_original_raw_'+key,lambda rows=rows:freezer.bootstrap_provenance(*rows))
z=copy.deepcopy(frozen);z['image_receipt']=fixture('closure-wrong-image-locator',dict(objects['images'],synthetic_changed_bytes=True))['path']
q=copy.deepcopy(proof);q['plan_preparation_sha256']=sha(encoded(z));rows=list(base);rows[1]=encoded(z);rows[3]=encoded(q)
bad('closure_actual_freezer_wrong_bytes_at_repinned_locator',lambda:freezer.bootstrap_provenance(*rows))
z=copy.deepcopy(plan);z['image']['192.168.0.111']='sha256:'+'e'*64;f=copy.deepcopy(frozen);f['plan']=z
q=copy.deepcopy(proof);q['plan_preparation_sha256']=sha(encoded(f));rows=list(base);rows[0]=encoded(z);rows[1]=encoded(f);rows[3]=encoded(q)
bad('closure_actual_freezer_repinned_plan_host_image',lambda:freezer.bootstrap_provenance(*rows))

# Invoke both actual local constructors for each internally consistent alternate
# product tuple. The original build/image/acceptance bytes never change.
for label in ('head','tree','inventory','server','driver','host111','host185','acceptance'):
 alternate=copy.deepcopy(normalized);reviewraw=Path(product_copy).read_bytes();inventory_pin=d['source_inventory']
 if label in ('head','tree'):alternate[label]='e'*40
 elif label=='inventory':
  inv=copy.deepcopy(objects['source_inventory']);inv['rows'][0]['git_blob']='e'*40;inventory_pin=fixture('closure-alternate-inventory',inv);alternate['source_inventory_sha256']=inventory_pin['sha256']
 elif label in ('server','driver'):alternate[label+'_sha256']='e'*64
 elif label in ('host111','host185'):
  nodes=('node-a','node-b') if label=='host111' else ('node-c','node-d')
  for node in nodes:alternate['server_images'][node]='sha256:'+'e'*64
  if label=='host185':alternate['driver_image']='sha256:'+'e'*64
 else:reviewraw=encoded(dict(objects['source_prereview'],synthetic_alternate_bytes=True))
 ar=fixtures/('trial24-alternate-product-'+label);br=ar/'binding-receipts-root-v1';br.mkdir(parents=True)
 raw=encoded(alternate);(br/'build.json').write_bytes(raw);(br/'source-prereview.json').write_bytes(reviewraw)
 gpins=copy.deepcopy(growth_pins);gpins.update(artifact_root=str(ar),source_head=alternate['head'],source_tree=alternate['tree'])
 for old,key in (('b625421c06ae6f9398c384686f3c50ce2389ac21','head'),('223df16137b4d11565a4a5ed033973c40ffa7556','tree'),('7ec5f0ed4f36ca69e355abc8c8c0d44920808bdce1c864482544a462b8453e04','source_inventory_sha256'),('701c02e0688ac0e923ceaff239c764ee2678fc993a12db086ce5deec7677744a','driver_sha256'),('64877e9d0163bed36494442c1c1799150babc21f54fc322e760e70c08fd8427f','server_sha256'),('sha256:718a79a455321acc6409436db4de44414ed8c4dd42e5ca684e9abf0a664889f8','driver_image')):
  gpins['literal_bindings'][old]=alternate[key]
 gpins['literal_bindings'].update({'sha256:5071d652bdb56e152ca9cdba581e88b00bf513e1b17ed9e3439e0cfb8ffcf9a5':alternate['server_images']['node-a'],'2c9b6e24ab9cca255ff4d36bc3b989bf3b60bafa74192c6518b0d1e854701da3':sha(reviewraw),'c31d6cb04c845616618c37d9d70d3af4b322b2b926cadcb1cfb98c37c53ed7ed':sha(raw)})
 ppins=copy.deepcopy(post_pins);ppins.update(source_head=alternate['head'],source_tree=alternate['tree'])
 ppins['constants'].update(HEAD=alternate['head'],TREE=alternate['tree'],GO_SHA=alternate['source_inventory_sha256'],SERVER_SHA=alternate['server_sha256'],DRIVER_SHA=alternate['driver_sha256'],BUILD=str(br/'build.json'),BUILD_SHA=sha(raw),SOURCE_REVIEW=str(br/'source-prereview.json'),SOURCE_REVIEW_SHA=sha(reviewraw),SOURCE_INV=inventory_pin['path'])
 for role,pins in (('growth_constructor',gpins),('post_constructor',ppins)):
  proc,target=generated(role,pins,'closure-'+role+'-alternate-'+label)
  assert proc.returncode!=0 and not target.exists(),(role,label,proc.stderr)
  checks.append('closure_actual_'+role+'_rejects_mutually_repinned_'+label)
for role,pins in (('growth_constructor',growth_pins),('post_constructor',post_pins)):
 proc,target=generated(role,pins,'closure-'+role+'-normalized-positive')
 assert proc.returncode==0,proc.stderr
 checks.append('closure_actual_'+role+'_distinct_normalized_raw_SHA_positive')
# Actual emitted admission's product-read/join prefix, before later generated
# source review/admission or any artifact/runtime call.
admission_path=growth_out/('gomap-4997-4998-trial24-growth-admission-root-v1.py')
admission_tree=ast.parse(admission_path.read_bytes());admit=next(n for n in admission_tree.body if isinstance(n,ast.FunctionDef) and n.name=='admit')
start=next(i for i,n in enumerate(admit.body) if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id=='product')
stop=next(i for i,n in enumerate(admit.body) if isinstance(n,ast.Assert) and 'review' in ast.unparse(n.test) and 'ACCEPT_BOOTSTRAP' in ast.unparse(n.test))
namespace={'pinned':lambda path,digest:json.loads(bp.read(path,digest)),'EXPECTED':normalized|{'source_head':head,'source_tree':tree,'query_image':normalized['driver_image']}}
exec(compile(ast.Module(body=admit.body[start:stop],type_ignores=[]),'actual-generated-growth-product-prefix','exec'),namespace)
checks.append('closure_actual_generated_growth_product_prefix_strict_tuple_positive')
(fixtures/'receipt-closure-integration.json').write_text(json.dumps({'state':'SYNTHETIC_ACTUAL_PRODUCER_RECEIPT_CLOSURE_ONLY','product':normalized,'original_build_sha256':d['build']['sha256'],'normalized_build_sha256':sha(normalized_raw),'images_sha256':d['images']['sha256'],'product_acceptance_sha256':d['source_prereview']['sha256'],'actual_bootstrap_plan':str(plan_root),'lifecycle_cases':closure_receipts,'added_controls':checks[291:],'runtime_started':False,'Go_started':False,'network_calls':0,'limits':['No real final product acceptance envelope exists. All acceptance-shaped inputs here are synthetic.','Bootstrap dataset/TLS dependencies are controlled; no certificate, dataset, external Git/SSH or infrastructure acceptance runs.','Actual lifecycle prefix stops after first owned mkdir; full lifecycle and cluster never execute.','Generated-growth source review remains a separate later gate.']},indent=2)+'\n')
