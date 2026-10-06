if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import sys
sys.dont_write_bytecode=True
from source_paths import source_path, isolate_paths, reviewer_identity
"""Inert Trial24 bootstrap constructor. Root supplies final accepted pins; no network here."""
import argparse,hashlib,importlib.util,json,pathlib,re,subprocess,ssl
CAMPAIGN='rf4trial24mixedchangingc1'
PREFIX='gomap-4997-4998-rf4trial24mixedchangingc1'
LAUNCHER=pathlib.Path(source_path('/tmp/gomap-4956-5fa-fixed-cluster.py'))
LAUNCHER_SHA='f5c92099cc94855f2bb5c0cd8ccd61902307386bebf4f15f2fd34581fabc0605'
def read(path,digest=None):
 p=pathlib.Path(path);assert p.is_absolute() and p.is_file() and not p.is_symlink() and p.stat().st_size<=8*1024*1024
 raw=p.read_bytes();assert digest is None or hashlib.sha256(raw).hexdigest()==digest
 return raw

PRODUCT_PATHS={'build':'__ROOT_FROZEN_BUILD_PATH__','images':'__ROOT_FROZEN_IMAGES_PATH__','source_acceptance':'__ROOT_FROZEN_ACCEPTANCE_PATH__'}
PRODUCT_SHA={'build':'__ROOT_FROZEN_BUILD_SHA__','images':'__ROOT_FROZEN_IMAGES_SHA__','source_acceptance':'__ROOT_FROZEN_ACCEPTANCE_SHA__'}
def elf_identity(records):
 # Build-only locators/tool evidence remain authenticated by the raw proof SHA.
 # Packaging retains the executable identity projection, not build metadata.
 assert set(records)=={'treedb-fixed-peer','treedb-query-under-write'}
 identity={}
 for name,record in records.items():
  digest=record['sha256'];size=record['bytes']
  assert isinstance(digest,str) and re.fullmatch('[0-9a-f]{64}',digest) and digest!='0'*64
  assert type(size) is int and size>0
  identity[name]={'sha256':digest,'bytes':size}
 return identity
def review_paths(accept):
 refs=accept['underlying_reviews'];assert isinstance(refs,list) and len(refs)>=2
 identities=set();digests=set();paths=[]
 for row in refs:
  assert isinstance(row,dict) and set(row)=={'path','sha256'} and re.fullmatch('[0-9a-f]{64}',row['sha256'])
  raw=read(row['path'],row['sha256']);identity=reviewer_identity(json.loads(raw))
  assert identity not in identities and row['sha256'] not in digests
  identities.add(identity);digests.add(row['sha256']);paths.append(row['path'])
 return paths
def product_review_paths(paths=None):
 paths=PRODUCT_PATHS if paths is None else paths
 return review_paths(json.loads(read(paths['source_acceptance'],PRODUCT_SHA['source_acceptance'])))

def product_tuple(buildraw,imagesraw,acceptanceraw):
 assert hashlib.sha256(buildraw).hexdigest()==PRODUCT_SHA['build']
 assert hashlib.sha256(imagesraw).hexdigest()==PRODUCT_SHA['images']
 assert hashlib.sha256(acceptanceraw).hexdigest()==PRODUCT_SHA['source_acceptance']
 build=json.loads(buildraw);images=json.loads(imagesraw);accept=json.loads(acceptanceraw)
 assert build['state']=='VERIFIED_ELFS_NOT_IMAGES_OR_RUNTIME' and build['source_verified_before_after'] is True
 assert build['head']==images['head']==accept['candidate_head']=='__ROOT_FROZEN_HEAD__'
 assert build['tree']==images['tree']==accept['candidate_tree']=='__ROOT_FROZEN_TREE__'
 assert build['source_inventory_sha256']==images['source_inventory_sha256']==accept['source_inventory_sha256']=='__ROOT_FROZEN_INVENTORY_SHA__'
 assert images['state']=='BOTH_HOSTS_PACKAGED_NOT_RUNTIME' and images['build_proof_sha256']==PRODUCT_SHA['build']
 assert accept['outcome']=='ACCEPT' and accept['landed_source_verified'] is True and accept['required_ci_passed'] is True and len(accept['underlying_reviews'])>=2
 review_paths(accept)
 identity=elf_identity(build['ELFs'])
 server=identity['treedb-fixed-peer']['sha256'];driver=identity['treedb-query-under-write']['sha256']
 assert server=='__ROOT_FROZEN_SERVER_SHA__' and driver=='__ROOT_FROZEN_DRIVER_SHA__'
 assert set(images['images'])=={'111','185'}
 hosts={}
 for host in ('111','185'):
  row=images['images'][host]
  assert row['host']=='192.168.0.'+host and row['state']=='SOURCE_VERIFIED_ELFS_PACKAGED_NO_CLUSTER_QUALIFICATION'
  assert row['head']==build['head'] and row['tree']==build['tree'] and row['source_inventory_sha256']==build['source_inventory_sha256']
  assert elf_identity(row['ELFs'])==identity and row['parent_unchanged'] is True and row['stores_mounted'] is False
  hosts[host]=row['image']
 assert hosts=={'111':'__ROOT_FROZEN_IMAGE111__','185':'__ROOT_FROZEN_IMAGE185__'}
 return {'state':'VERIFIED_BUILD_FROM_FROZEN_SOURCE','head':build['head'],'tree':build['tree'],'source_inventory_sha256':build['source_inventory_sha256'],'server_sha256':server,'driver_sha256':driver,'server_images':{'node-a':hosts['111'],'node-b':hosts['111'],'node-c':hosts['185'],'node-d':hosts['185']},'driver_image':hosts['185'],'source_verified_before_after':True}
def frozen_product(paths=None):
 paths=PRODUCT_PATHS if paths is None else paths
 assert set(paths)==set(PRODUCT_PATHS)
 return product_tuple(*[read(paths[k],PRODUCT_SHA[k]) for k in ('build','images','source_acceptance')])
def normalized_product(raw,acceptanceraw,expected):
 # Normalized bytes have their own later SHA and schema, never the build proof SHA.
 value=json.loads(raw)
 assert set(value)==set(expected) and value==expected and value['source_verified_before_after'] is True
 assert hashlib.sha256(acceptanceraw).hexdigest()==PRODUCT_SHA['source_acceptance']
 return value
def preparation_paths(frozen):
 return {k:frozen[field] for k,field in (('build','build_receipt'),('images','image_receipt'),('source_acceptance','source_acceptance'))}
def preparation_tuple(frozen,a):
 assert frozen['source_head']==a['head']=='__ROOT_FROZEN_HEAD__' and frozen['source_tree']==a['tree']=='__ROOT_FROZEN_TREE__'
 assert frozen['source_inventory_sha256']=='__ROOT_FROZEN_INVENTORY_SHA__'
 for key,field in (('build','build_receipt_sha256'),('images','images_receipt_sha256'),('source_acceptance','source_review_sha256')):
  assert frozen[field]==a[key+'_sha256']==PRODUCT_SHA[key]
 assert frozen['product']==frozen_product(preparation_paths(frozen))
def preflight_tuple(proof,frozen,a):
 preparation_tuple(frozen,a)
 assert proof['runtime_source_head']==a['head'] and proof['runtime_source_tree']==a['tree']
 assert proof['source_inventory_sha256']==frozen['source_inventory_sha256']
 for field in ('build_receipt_sha256','images_receipt_sha256','source_review_sha256'):
  assert proof[field]==frozen[field]
 assert proof['daemon_sha256']=='__ROOT_FROZEN_SERVER_SHA__' and proof['driver_sha256']=='__ROOT_FROZEN_DRIVER_SHA__'
 assert proof['server_images']==frozen['product']['server_images'] and proof['driver_image']==frozen['product']['driver_image']

def protected_inputs(a,pinfile):
 return [a['source_worktree'],pathlib.Path(__file__).resolve().parent,pinfile,a['dataset'],a['build'],a['images'],a['source_acceptance'],a['credential_provenance']]+[v['path'] for v in a['configs'].values()]+product_review_paths({k:a[k] for k in PRODUCT_PATHS})

def context(pinfile):
 assert __debug__
 a=json.loads(read(pinfile));assert a['RootAcceptedFinalSourceAndInputs'] is True
 for k in ('head','tree'):assert re.fullmatch('[0-9a-f]{40}',a[k])
 for k in ('build','images','source_acceptance'):assert a[k+'_sha256']==PRODUCT_SHA[k]
 product=frozen_product({k:a[k] for k in PRODUCT_PATHS})
 build=json.loads(read(a['build'],a['build_sha256']));images=json.loads(read(a['images'],a['images_sha256']));accept=json.loads(read(a['source_acceptance'],a['source_acceptance_sha256']))
 a['product']=product
 assert build['state']=='VERIFIED_ELFS_NOT_IMAGES_OR_RUNTIME' and build['source_verified_before_after'] is True
 assert a['build_sha256']=='__ROOT_FROZEN_BUILD_SHA__' and build['ELFs']['treedb-fixed-peer']['sha256']=='__ROOT_FROZEN_SERVER_SHA__'
 assert a['head']=='__ROOT_FROZEN_HEAD__' and a['tree']=='__ROOT_FROZEN_TREE__'
 assert images['state']=='BOTH_HOSTS_PACKAGED_NOT_RUNTIME' and images['build_proof_sha256']==a['build_sha256']
 assert all(x['head']==a['head'] and x['tree']==a['tree'] for x in (build,images))
 assert images['source_inventory_sha256']==build['source_inventory_sha256']=='__ROOT_FROZEN_INVENTORY_SHA__'
 assert accept['outcome']=='ACCEPT' and accept['candidate_head']==a['head'] and accept['candidate_tree']==a['tree']
 assert accept['landed_source_verified'] is True and accept['required_ci_passed'] is True and len(accept['underlying_reviews'])>=2
 assert hashlib.sha256(read(LAUNCHER)).hexdigest()==LAUNCHER_SHA
 spec=importlib.util.spec_from_file_location('fixed_cluster',LAUNCHER);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
 assert set(images['images'])=={'111','185'} and set(a['configs'])=={'node-a','node-b','node-c','node-d'}
 nodes=[];config_sha={};credential_configs=[]
 for i,node in enumerate(('node-a','node-b','node-c','node-d')):
  meta=a['configs'][node];assert re.fullmatch('[0-9a-f]{64}',meta['sha256'])
  p=pathlib.Path(meta['path']);assert str(p).startswith('/tmp/'+PREFIX+'-')
  x=json.loads(read(p,meta['sha256']));assert x['NodeID']==node;credential_configs.append(x)
  base='/home/mikers/gomap-4250-twohost-'+CAMPAIGN+'/'+node
  assert x['DataRoot']==base+'/data' and x['RaftRoot']==base+'/raft'
  assert x['ListenAddress']=='192.168.0.'+('111' if i<2 else '185')+':'+str(19101+i)
  assert x['RaftListen']=={'meta':'192.168.0.'+('111' if i<2 else '185')+':'+str(19201+i),'group-a':'192.168.0.'+('111' if i<2 else '185')+':'+str(19301+i)}
  assert x['VectorInitialization']['IndexDefinition']=={'name':'embedding_graph','field':'embedding','metric':'cosine','dimensions':128,'m':16,'ef_construction':128,'ef_search':128,'strategy':'column_graph'}
  assert x['VectorInitialization']['MaxSourceRows']==10003
  hosts={'node-a':'192.168.0.111','node-b':'192.168.0.111','node-c':'192.168.0.185','node-d':'192.168.0.185'}
  assert x['VectorInitialization']['PublicAddresses']=={n:'127.0.0.1:'+str(19401+j) for j,n in enumerate(hosts)}
  assert x['VectorInitialization']['ShardAddresses']=={'group-a':{n:h+':'+str(19501+j) for j,(n,h) in enumerate(hosts.items())}}
  assert {n['ID']:n['Address'] for n in x['Nodes']}=={n:h+':'+str(19101+j) for j,(n,h) in enumerate(hosts.items())}
  assert {n['ID']:n['Address'] for n in x['Catalog']['Peers']}=={n:h+':'+str(19201+j) for j,(n,h) in enumerate(hosts.items())}
  assert {n['ID']:n['Address'] for n in x['Groups'][0]['Peers']}=={n:h+':'+str(19301+j) for j,(n,h) in enumerate(hosts.items())}
  config_sha[str(p)]=meta['sha256'];nodes.append({'host':m.HOSTS[0 if i<2 else 1],'config':str(p)})
 guard_path=pathlib.Path(source_path('/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-rf4trial24-credential-preactivation-guard-root-v1.py'))
 assert hashlib.sha256(read(guard_path)).hexdigest()=='bbca8e84b31b0c80506586dbba511168075bfc6fc9af1b1ec70b43d942ca8e9a'
 gs=importlib.util.spec_from_file_location('fresh_credential_guard',guard_path);g=importlib.util.module_from_spec(gs);gs.loader.exec_module(g)
 assert a['credential_minimum_remaining_seconds']==86400
 proof=g.validate_configs(credential_configs,minimum_remaining_seconds=86400)
 provenance=json.loads(read(a['credential_provenance'],a['credential_provenance_sha256']))
 assert provenance['cluster']==proof['cluster_id'] and provenance['days']==7
 assert {v['node']:v['sha256'] for v in provenance['configs']}=={n:v['sha256'] for n,v in a['configs'].items()}
 public={v['path']:v for v in provenance['public_certificates']}
 for v in proof['public_credentials']:
  assert public[v['trust_path']]['sha256']==v['trust_sha256'] and public[v['certificate_path']]['sha256']==v['certificate_sha256']
 assert max(ssl.cert_time_to_seconds(v['metadata']['notBefore']) for v in public.values())==a['credential_not_before_unix']
 assert min(ssl.cert_time_to_seconds(v['metadata']['notAfter']) for v in public.values())==a['credential_not_after_unix']
 a['credential_validation']=proof
 manifest={'image':{'192.168.0.'+h:x['image'] for h,x in images['images'].items()},'binary':'/treedb-fixed-peer','binary_sha256':build['ELFs']['treedb-fixed-peer']['sha256'],'nodes':nodes}
 for h,x in images['images'].items():
  assert x['head']==a['head'] and x['tree']==a['tree'] and x['parent_unchanged'] is True and x['stores_mounted'] is False
  assert x['ELFs']['treedb-fixed-peer']['sha256']==manifest['binary_sha256']
  assert x['ELFs']['treedb-query-under-write']['sha256']==build['ELFs']['treedb-query-under-write']['sha256']
 dataset=m.admit_dataset(a['dataset']);assert dataset['Rows']==10000 and dataset['Dimensions']==128 and dataset['SourceRows']==10003
 assert dataset['ManifestSHA256']==a['dataset_manifest_sha256'] and dataset['VectorsSHA256']==a['dataset_vectors_sha256']
 dm=m.read_json(pathlib.Path(a['dataset'])/'manifest.json');assert dm['queries']==16
 planned=m.plan(manifest,CAMPAIGN,dataset);assert len(planned)==4
 assert {n['node'] for n in planned}=={'node-a','node-b','node-c','node-d'}
 for n in planned:assert n['root'].startswith('/home/mikers/gomap-4250-twohost-'+CAMPAIGN+'/')
 return a,m,manifest,dataset,config_sha

def main():
 p=argparse.ArgumentParser();p.add_argument('--pins',required=True);p.add_argument('--out',required=True);args=p.parse_args()
 a,m,manifest,dataset,configs=context(args.pins)
 root=pathlib.Path(args.out);assert root.is_absolute() and root.name.startswith(PREFIX+'-bootstrap-plan-root-v') and not root.exists()
 isolate_paths([root],protected_inputs(a,args.pins))
 root.mkdir();(root/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
 planned={'provisional':True,'workflow':['inspect4','serve4','initialize','stop4','start4','qualify'],'image':manifest['image'],'binary_sha256':manifest['binary_sha256'],'nodes':[{k:n[k] for k in ('host','image','node','root','name')} for n in m.plan(manifest,CAMPAIGN,dataset)],'driver_node':'node-c','planned_source_rows':10003,'planned_total_documents':10004,'dataset':dataset,'operation_timeout_seconds':600}
 (root/'plan.json').write_text(json.dumps(planned,indent=2)+'\n')
 receipt={'build_receipt':a['build'],'build_receipt_sha256':a['build_sha256'],'config_sha256':configs,'manifest_sha256':hashlib.sha256(read(root/'manifest.json')).hexdigest(),'source_head':a['head'],'source_tree':a['tree'],'image_receipt':a['images'],'images_receipt_sha256':a['images_sha256'],'source_acceptance':a['source_acceptance'],'source_review_sha256':a['source_acceptance_sha256'],'source_inventory_sha256':'__ROOT_FROZEN_INVENTORY_SHA__','product':a['product'],'plan':planned,'state':'PREPARATION_ONLY_NOT_BOOTSTRAP_ACCEPTANCE','pins_sha256':hashlib.sha256(read(args.pins)).hexdigest(),'launcher_sha256':LAUNCHER_SHA}
 (root/'plan-preparation.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt))
if __name__=='__main__':main()
