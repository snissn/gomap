"""Bounded synthetic source construction checks. No transport or ranking."""
import argparse,ast,base64,copy,difflib,hashlib,importlib.util,json,math,re,struct,subprocess,sys,uuid
from pathlib import Path
sys.dont_write_bytecode=True
R=Path(__file__).parent
ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('--fixtures-root',type=Path);args=ap.parse_args()
checks=[]
def sha(b):return hashlib.sha256(b).hexdigest()
def good(name,f):f();checks.append(name)
def bad(name,f):
 try:f()
 except (AssertionError,ValueError,TypeError,KeyError,IndexError):checks.append(name);return
 raise AssertionError('accepted invalid fixture '+name)
def load(path,name):
 spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
m=json.loads((R/'packet.json').read_bytes())
for role,row in m['roles'].items():
 new=(R/row['output']['path']).read_bytes()
 assert sha(new)==row['output']['sha256'];ast.parse(new)
 assert re.fullmatch('[0-9a-f]{64}',row['original']['sha256'])
 assert b'trial18' not in new and b'Trial18' not in new and b'TRIAL18' not in new
checks.append('all15_original_and_candidate_bytes_AST_fresh_namespace')
c=load(R/m['roles']['collector']['output']['path'],'trial24_inert_synthetic_collector')
c.call=lambda *a,**kw:(_ for _ in ()).throw(AssertionError('transport prohibited'))
x=load(R/'instantiate.py','trial24_source_instantiator')
assert x.PACKET_SHA==sha((R/'packet.json').read_bytes())
bad('missing_final_declaration',lambda:x.declaration(json.loads((R/'declaration-template.json').read_bytes()),m))
# Exact JSON integers, not floats or bools, including every uint64 prefix bit.
assert c.strict_json('9223372036854775808')==1<<63
assert c.uint64_mask(c.strict_json('18446744073709551615'))==(1<<64)-1
checks.append('JSON_exact_bit63_and_all64_uint64_bits')
for label,v in [('mask_zero',0),('mask_overflow',1<<64),('mask_negative',-1),('mask_float',float(1<<63)),('mask_bool',True)]:bad(label,lambda v=v:c.uint64_mask(v))
assert c.original_count({})==c.original_count({'Originals':0})==6
for v in (6,58,63):assert c.original_count({'Originals':v})==v
for v in (True,5,64,58.0,-1):bad('invalid_Originals_'+repr(v),lambda v=v:c.original_count({'Originals':v}))
# Whole-response prefix compatibility includes bit63 and all64 bits. No Python rank.
neighbors=[{'ID':'n%d'%i,'Score':1-i/16} for i in range(10)]
response={'Generation':{},'Neighbors':neighbors,'Counters':{'SelectedPartitions':1,'HNSWServedPartitions':1,'ReadProofs':1,'ExactScanPartitions':0,'Retries':0,'Redirects':0}}
changed=[{'ID':'absent%d'%i,'Present':False,'ScoreBits':0} for i in range(4)]
prefixes=[{'Prefix':i,'Changed':[copy.deepcopy(changed)],'Truth':[copy.deepcopy(neighbors)]} for i in range(64)]
assert c.compatible_prefixes(response,prefixes,0,0,63)==((1<<64)-1,1.0)
assert c.compatible_prefixes(response,prefixes,0,63,63)==(1<<63,1.0)
checks.extend(['all64_compatible_states','only_prefix63_compatible'])
prefixes[63]['Truth'][0]=neighbors[:5]+[{'ID':'other%d'%i,'Score':0} for i in range(5)]
assert c.compatible_prefixes(response,prefixes,0,0,63)[1]==.5
checks.append('minimum_across_every_compatible_state')
bad('prefix65_cap',lambda:c.compatible_prefixes(response,prefixes+[prefixes[-1]],0,0,64))
bad('bool_causal_bound',lambda:c.compatible_prefixes(response,prefixes,0,False,63))
# Real omitted-pointer request schema, original six shape and52 changing replacements.
vecs=[[1.,-0.0]+[0.]*126,[-1.,-0.0]+[0.]*126,[0.,1.]+[0.]*126]
pattern=[('replace',0,0),('replace',0,1),('delete',1,None),('replace',2,2),('delete',2,None),('delete',3,None)]+[('replace',0,i%2) for i in range(6,58)]
writes=[]
for i,(kind,id,vi) in enumerate(pattern):
 req={'Version':1,'Generation':{'Index':'synthetic','Generation':2},'ID':base64.b64encode(('doc-%06d'%id).encode()).decode(),'IdempotencyKey':base64.b64encode(('trial24-key-%d'%i).encode()).decode(),'Deadline':'0001-01-01T00:00:00Z'}
 if kind=='replace':req.update(Vector=vecs[vi],Document=base64.b64encode(c.encode_go_json({'embedding':vecs[vi]})).decode())
 writes.append({'Ordinal':i,'Kind':kind,kind.capitalize():req,'IntendedOffsetNS':i*5000000000,'Outcome':'succeeded','Invoked':True})
report={'Originals':58,'Writes':writes,'Prefixes':[{'Prefix':i,'PopulationRows':10005-(i>=3)-(i>=5)-(i>=6),'Truth':[[]]*16,'Changed':[[]]*16} for i in range(59)],'Concurrency':1,'WarmupPlanned':64,'MaxAttempts':65536,'OutputBytes':134217728,'RequestedDuration':300000000000,'PaceInterval':5000000000,'Truncated':False,'Retries':[dict(writes[0]),dict(writes[2])],'ReadPrefixes':[{'CompatibleMask':1<<58}]}
c.sustained_shape(report);checks.append('complete58_originals59_prefix_shape')
for label,edit in [('missing_original',lambda z:z['Writes'].pop()),('duplicate_ordinal',lambda z:z['Writes'][-1].__setitem__('Ordinal',56)),('missing_prefix',lambda z:z['Prefixes'].pop()),('duplicate_prefix',lambda z:z['Prefixes'][-1].__setitem__('Prefix',57)),('wrong_count',lambda z:z.__setitem__('Originals',57)),('truncated',lambda z:z.__setitem__('Truncated',True)),('output_cap',lambda z:z.__setitem__('OutputBytes',134217729)),('unknown_original',lambda z:z['Writes'][6].__setitem__('Outcome','unknown')),('duplicate_key',lambda z:z['Writes'][6]['Replace'].__setitem__('IdempotencyKey',z['Writes'][0]['Replace']['IdempotencyKey'])),('continued_noop',lambda z:z['Writes'][6]['Replace'].__setitem__('Vector',vecs[1])),('deleted_ID_reuse',lambda z:z['Writes'][6]['Replace'].__setitem__('ID',z['Writes'][2]['Delete']['ID'])),('wrong_retry',lambda z:z['Retries'][1].__setitem__('Ordinal',1)),('float_mask',lambda z:z['ReadPrefixes'][0].__setitem__('CompatibleMask',1.0))]:
 z=copy.deepcopy(report);edit(z);bad(label,lambda z=z:c.sustained_shape(z))
assert c.strict_json('[1,-0]')[1]==0 and struct.pack('<f',c.strict_json('[1,-0]')[1])==struct.pack('<f',-0.0)
checks.append('raw_JSON_negative_zero_FP32_retained')
z=copy.deepcopy(writes[0]);z['Delete']=None;bad('inactive_pointer_null',lambda:c.original_requests([z]))
bad('duplicate_JSON',lambda:c.strict_json('{"Originals":58,"Originals":6}'))
bad('nonfinite_JSON',lambda:c.strict_json('{"Score":NaN}'))
# Reused focused pure checks exercise actual adapted read/audit loading/permission/resource.
ra=load(R/m['roles']['read_audit']['output']['path'],'trial24_read_audit_pure');ra.self_check();checks.append('changing_read_audit10_pure_controls')
ledger=[{'Outcome':'succeeded','Invoked':True,'StartNS':i*10+1,'EndNS':i*10+2} for i in range(63)]
assert ra.rd.causal_range(ledger,633,634)==(63,63);checks.append('actual_adapted_causal_range_all63_writes')
bad('causal_64_write_cap',lambda:ra.rd.causal_range(ledger+[ledger[-1]],643,644))
rs=load(R/m['roles']['resource']['output']['path'],'trial24_resource_pure');rs.self_check();checks.append('resource_permission49_pure_controls')
c.self_check();checks.append('collector30_omitted_pointer_and_population_pure_controls')
# Exercise the actual external exit snippet without opening a campaign path.
resource_source=(R/m['roles']['resource']['output']['path']).read_text()
fn=next(v for v in ast.parse(resource_source).body if isinstance(v,ast.FunctionDef) and v.name=='verifier')
start=next(i for i,v in enumerate(fn.body) if isinstance(v,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='ce' for t in v.targets))
exit_source='\n'.join(ast.get_source_segment(resource_source,v) for v in fn.body[start:start+3])
def exit_guard(raw,digest):
 ns=dict(rs.__dict__,bounded=lambda p,cap:raw,collector_exit_path='synthetic-only',collector_exit_sha256=digest)
 exec(compile(exit_source,'actual-resource-exit-guard','exec'),ns)
exit_guard(b'0\n',sha(b'0\n'));checks.append('actual_external_exit0_guard')
bad('actual_external_nonzero_exit',lambda:exit_guard(b'1\n',sha(b'1\n')))
bad('actual_external_exit_raw_hash_mismatch',lambda:exit_guard(b'0\n','0'*64))
# Synthetic final-pin declaration tests exercise the real pre-output validator.
fixtures=args.fixtures_root or R/'synthetic-final-pin-fixtures'
fixtures.mkdir(exist_ok=True)
head,tree='1'*40,'2'*40
objects={'source_inventory':{'head':head,'tree':tree,'rows':[{'path':'synthetic.go','mode':'100644','git_blob':'3'*40}],'overlays':{}},'build':{'head':head,'tree':tree,'source_inventory_sha256':None,'state':'VERIFIED_ELFS_NOT_IMAGES_OR_RUNTIME','source_verified_before_after':True,'ELFs':{'treedb-fixed-peer':{'sha256':'4'*64,'bytes':100},'treedb-query-under-write':{'sha256':'5'*64,'bytes':200}}},'images':{'head':head,'tree':tree,'source_inventory_sha256':None},'source_prereview':{'decision':'ACCEPT','candidate_head':head,'candidate_tree':tree}}
def fixture(name,obj):
 path=fixtures/(name+'.json');raw=json.dumps(obj).encode();path.write_bytes(raw);return {'path':str(path),'sha256':sha(raw)}
d={'Version':1,'campaign':m['campaign'],'workload':m['workload'],'source_head':head,'source_tree':tree,'source_root':'/synthetic-final-source-not-runtime','RootAcceptedFinalSourcePins':True,'output_root':'/tmp/synthetic-trial24-output-not-created'}
d['source_inventory']=fixture('source_inventory',objects['source_inventory'])
for k in ('build','images'):objects[k]['source_inventory_sha256']=d['source_inventory']['sha256']
d['build']=fixture('build',objects['build'])
objects['images'].update(state='BOTH_HOSTS_PACKAGED_NOT_RUNTIME',build_proof_sha256=d['build']['sha256'],images={})
for host in ('111','185'):
 objects['images']['images'][host]={'state':'SOURCE_VERIFIED_ELFS_PACKAGED_NO_CLUSTER_QUALIFICATION','host':'192.168.0.'+host,'image':'sha256:'+'7'*64,'parent_unchanged':True,'stores_mounted':False,'ELFs':copy.deepcopy(objects['build']['ELFs']),'head':head,'tree':tree,'source_inventory_sha256':d['source_inventory']['sha256']}
for k in ('images','source_prereview'):d[k]=fixture(k,objects[k])
x.declaration(d,m);checks.append('explicit_final_pin_validator_synthetic_only')
z=copy.deepcopy(d);z['source_head']='6'*40;bad('final_source_head_mismatch',lambda:x.declaration(z,m))
z=copy.deepcopy(d);z['source_inventory']['sha256']='0'*64;bad('final_inventory_actual_byte_hash_mismatch',lambda:x.declaration(z,m))
z=copy.deepcopy(d);z['workload']['OutputBytes']+=1;bad('undeclared_cap_expansion',lambda:x.declaration(z,m))
z=copy.deepcopy(d);z['RootAcceptedFinalSourcePins']=False;bad('missing_root_final_acceptance',lambda:x.declaration(z,m))

assert ra.mutation_adapted.count("3000000000")==1 and "10000000000" not in ra.mutation_adapted
checks.append("actual_mutation_deadline_explicit_RPC3s")

# Actual command flags and no silent bounds expansion.
ns=rs.pure_collector({'query_image':'sha256:'+'1'*64,'driver_uid_gid':'1000:1000','driver_sha256':'2'*64},'a'*32)
argv=ns['driver_arguments']()
for flag,want in [('-mixed-originals','58'),('-read-window','300s'),('-mixed-interval','5s'),('-timeout','420s'),('-rpc-timeout','3s'),('-read-warmup','64'),('-read-max-attempts','65536'),('-read-output-bytes','134217728')]:assert argv.count(flag)==1 and argv[argv.index(flag)+1]==want
checks.append('exact_sustained_argv_existing_caps')
# Exact immutable portable transitive source closure.
for row in m['transitive_sources'].values():
 raw=(R/row['path']).read_bytes();assert sha(raw)==row['sha256']
 if row['path'].endswith('.py'):ast.parse(raw)
assert sha((R/m['source_path_resolver']['path']).read_bytes())==m['source_path_resolver']['sha256']
checks.append('all14_transitive_sources_and_resolver_actual_byte_pins')
for row in m['root_owned_adapters'].values():
 raw=(R/row['path']).read_bytes();assert sha(raw)==row['sha256'];ast.parse(raw)
checks.append('both_root_owned_inert_adapters_actual_source_hashes_AST_only')

# Compile the actual assembled remote programs, not only the outer adapter.
# This catches indentation inside multiline payloads before transport exists.
a=ast.parse((R/m['root_owned_adapters']['runtime-build-prepare-root-v1.py']['path']).read_bytes())
f=next(v for v in a.body if isinstance(v,ast.FunctionDef) and v.name=='main')
assign={v.targets[0].id:v.value for v in f.body if isinstance(v,ast.Assign) and isinstance(v.targets[0],ast.Name)}
verify=ast.literal_eval(assign['verify'])
for name in ('prepare','run','finish','capture'):
 ast.parse('import hashlib,json,os,pathlib,subprocess,sys\n'+verify+ast.literal_eval(assign[name].right))
checks.append('actual_all4_assembled_build_remote_programs_AST_before_transport')

# Existing receipt schema, using synthetic-only bytes and the real validator.
def rejected_pin(label,key,edit):
 obj=copy.deepcopy(objects[key]);edit(obj);z=copy.deepcopy(d);z[key]=fixture('negative-'+label,obj)
 bad(label,lambda:x.declaration(z,m))
rejected_pin('images_metadata_only','images',lambda z:[z.pop(k) for k in ('state','build_proof_sha256','images')])
rejected_pin('images_wrong_state','images',lambda z:z.__setitem__('state','RUNTIME'))
rejected_pin('images_wrong_build_receipt','images',lambda z:z.__setitem__('build_proof_sha256','8'*64))
rejected_pin('images_missing_host','images',lambda z:z['images'].pop('185'))
rejected_pin('images_extra_host','images',lambda z:z['images'].__setitem__('186',copy.deepcopy(z['images']['185'])))
for host in ('111','185'):
 for key,value in [('state','RUNTIME'),('host','192.168.0.186'),('image','sha256:'+'0'*64),('image','sha256:'+'8'*63),('head','8'*40),('tree','8'*40),('source_inventory_sha256','8'*64),('parent_unchanged',False),('parent_unchanged',1),('stores_mounted',True),('stores_mounted',0)]:
  rejected_pin('host'+host+'_'+key+'_'+repr(value),'images',lambda z,key=key,value=value,host=host:z['images'][host].__setitem__(key,value))
 for name in ('treedb-fixed-peer','treedb-query-under-write'):
  for key,value in [('sha256','8'*64),('sha256','0'*64),('bytes',0),('bytes',-1),('bytes',True),('bytes',100.0),('bytes',999)]:
   rejected_pin('host'+host+'_'+name+'_'+key+'_'+repr(value),'images',lambda z,key=key,value=value,host=host,name=name:z['images'][host]['ELFs'][name].__setitem__(key,value))
  rejected_pin('host'+host+'_missing_'+name,'images',lambda z,host=host,name=name:z['images'][host]['ELFs'].pop(name))
rejected_pin('build_zero_ELF_bytes','build',lambda z:z['ELFs']['treedb-fixed-peer'].__setitem__('bytes',0))
rejected_pin('inventory_overlays_forbidden','source_inventory',lambda z:z.__setitem__('overlays',{'foreign':'x'}))
rejected_pin('inventory_duplicate_path','source_inventory',lambda z:z['rows'].append(copy.deepcopy(z['rows'][0])))
rejected_pin('inventory_empty','source_inventory',lambda z:z.__setitem__('rows',[]))
# Final row count is derived once from actual pinned inventory bytes.
bindings=x.declaration(d,m);assert bindings['"__ROOT_FROZEN_INVENTORY_ROWS__"']=='1'
checks.append('actual_declared_inventory_row_count_binding')
nr=load(R/m['roles']['native_runner']['output']['path'],'trial24_native_inert')
nr.HEAD=head;nr.TREE=tree;nr.INVENTORY_ROWS=1
nr.inventory_identity(objects['source_inventory']);checks.append('native_prepare_capture_shared_final_inventory_count_positive')
for wrong in (5770,5771,0,-1,True,1.0):
 nr.INVENTORY_ROWS=wrong;bad('native_prepare_capture_inventory_count_'+repr(wrong),lambda:nr.inventory_identity(objects['source_inventory']))
nr.INVENTORY_ROWS=1
# Execute only the actual first two remote source_verify statements, which
# perform the source identity/count join, against in-memory raw inventory.
remote=ast.parse(nr.REMOTE_COMMON);fn=next(n for n in remote.body if isinstance(n,ast.FunctionDef) and n.name=='source_verify')
join=next(i for i,n in enumerate(fn.body) if 'exact source identity' in ast.get_source_segment(nr.REMOTE_COMMON,n));fragment=ast.Module(body=fn.body[:join+1],type_ignores=[])
def remote_identity(count):
 inv=objects['source_inventory'];raw=json.dumps(inv).encode()
 ns=dict(nr.__dict__,ROOT=Path('/synthetic-no-IO'),SOURCE='/synthetic-no-IO',INV_SHA=sha(raw),INV_ROWS=count,read=lambda *a,**kw:raw)
 exec(compile(fragment,'actual-native-remote-inventory-join','exec'),ns)
remote_identity(1);checks.append('actual_native_remote_final_inventory_count_positive')
for wrong in (5770,5771,0,-1,True,1.0):bad('actual_native_remote_stale_count_'+repr(wrong),lambda wrong=wrong:remote_identity(wrong))
source=(R/m['roles']['native_runner']['output']['path']).read_text()
assert '5770' not in source and source.count('inventory_identity(')==3 and 'INV_ROWS=INVENTORY_ROWS' in source
checks.append('all3_native_inventory_joins_share_declared_count_no_stale_literal')
nr.self_check();checks.append('native_inert_source_tar_parser_checks_no_transport')

# Full real source-only instantiation into an explicitly synthetic namespace.
z=copy.deepcopy(d);z['output_root']=str(fixtures/('trial24-generated-'+uuid.uuid4().hex))
declaration=fixture('declaration',z)
proc=subprocess.run([sys.executable,'-B',str(R/'instantiate.py'),'--declaration',declaration['path'],'--declaration-sha256',declaration['sha256']],capture_output=True,text=True,timeout=15)
(fixtures/'instantiation.stdout').write_text(proc.stdout);(fixtures/'instantiation.stderr').write_text(proc.stderr);(fixtures/'instantiation.exit').write_text(str(proc.returncode)+'\n')
assert proc.returncode==0,proc.stderr
out=Path(z['output_root']);receipt=json.loads((out/'instantiation.json').read_bytes())
assert receipt['runtime_started'] is False and receipt['admission_flags_granted'] is False
assert len(receipt['roles'])==15
for role,row in receipt['roles'].items():
 raw=Path(row['path']).read_bytes();assert sha(raw)==row['sha256'];ast.parse(raw);assert b'__ROOT_FROZEN_' not in raw
nr_source=(out/Path(m['roles']['native_runner']['output']['path']).name).read_text()
assert 'INVENTORY_ROWS=1' in nr_source
for row in receipt['transitive_sources'].values():assert sha((out/row['path']).read_bytes())==row['sha256']
assert sha((out/receipt['source_path_resolver']['path']).read_bytes())==receipt['source_path_resolver']['sha256']
checks.append('actual_source_only_instantiation_exit0_all15_AST_bound_rows_and_transitive_pins')
# Fresh interpreter imports every emitted role with external process execution
# prohibited and every historical source path refused before file access.
import_program=r"""
import importlib.util,json,pathlib,subprocess,sys
sys.dont_write_bytecode=True
out=pathlib.Path(sys.argv[1]);sys.path.insert(0,str(out))
from source_paths import SOURCE_PATHS
original_open=pathlib.Path.open
def owned_open(self,*a,**kw):
 if str(self) in SOURCE_PATHS:raise AssertionError('historical source path accessed '+str(self))
 return original_open(self,*a,**kw)
pathlib.Path.open=owned_open
subprocess.Popen=lambda *a,**kw:(_ for _ in ()).throw(AssertionError('external processes prohibited'))
receipt=json.loads((out/'instantiation.json').read_bytes())
for role,row in receipt['roles'].items():
 spec=importlib.util.spec_from_file_location('synthetic_portable_'+role,row['path']);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
print(json.dumps({'all15_imported':True,'external_processes':0,'historical_source_reads':0}))
"""
proc=subprocess.run([sys.executable,'-B','-c',import_program,str(out)],capture_output=True,text=True,timeout=15)
(fixtures/'portable-imports.stdout').write_text(proc.stdout);(fixtures/'portable-imports.stderr').write_text(proc.stderr);(fixtures/'portable-imports.exit').write_text(str(proc.returncode)+'\n')
assert proc.returncode==0,proc.stderr
checks.append('actual_all15_generated_imports_exit0_no_historical_source_reads_or_external_processes')


print(json.dumps({'state':'AUTHOR_SYNTHETIC_SOURCE_CHECKS_PASS_NOT_INDEPENDENT_REVIEW','checks':checks,'count':len(checks),'runtime_started':False,'network_calls':0,'Go_started':False,'source_head':None,'source_tree':None,'limitations':['No actual final source pins, full native59-prefix run, timing/cap qualification, audit acquisition or campaign exists.','Guard shape fixture is synthetic; native Go remains sole ranking authority.']},indent=2))
