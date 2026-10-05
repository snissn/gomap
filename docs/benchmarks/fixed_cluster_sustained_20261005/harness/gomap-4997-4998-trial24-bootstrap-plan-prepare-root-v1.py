from source_paths import source_path, isolate_paths
"""Inert Trial24 bootstrap constructor. Root supplies final accepted pins; no network here."""
import argparse,hashlib,importlib.util,json,pathlib,re,subprocess,ssl
CAMPAIGN='rf4trial24mixedchangingc1'
PREFIX='gomap-4997-4998-rf4trial24mixedchangingc1'
LAUNCHER=pathlib.Path(source_path('/tmp/gomap-4956-5fa-fixed-cluster.py'))
LAUNCHER_SHA='21c3f9204489ae7179341772b4b856de2b9280acacfbe5ad4deea52da9750616'
def read(path,digest=None):
 p=pathlib.Path(path);assert p.is_absolute() and p.is_file() and not p.is_symlink() and p.stat().st_size<=8*1024*1024
 raw=p.read_bytes();assert digest is None or hashlib.sha256(raw).hexdigest()==digest
 return raw

def protected_inputs(a,pinfile):
 return [a['source_worktree'],pathlib.Path(__file__).resolve().parent,pinfile,a['dataset'],a['build'],a['images'],a['source_acceptance'],a['credential_provenance']]+[v['path'] for v in a['configs'].values()]

def context(pinfile):
 assert __debug__
 a=json.loads(read(pinfile));assert a['RootAcceptedFinalSourceAndInputs'] is True
 for k in ('head','tree'):assert re.fullmatch('[0-9a-f]{40}',a[k])
 for k in ('build','images','source_acceptance'):assert re.fullmatch('[0-9a-f]{64}',a[k+'_sha256'])
 build=json.loads(read(a['build'],a['build_sha256']));images=json.loads(read(a['images'],a['images_sha256']));accept=json.loads(read(a['source_acceptance'],a['source_acceptance_sha256']))
 assert build['state']=='VERIFIED_ELFS_NOT_IMAGES_OR_RUNTIME' and build['source_verified_before_after'] is True
 assert a['build_sha256']=='__ROOT_FROZEN_BUILD_SHA__' and build['ELFs']['treedb-fixed-peer']['sha256']=='__ROOT_FROZEN_SERVER_SHA__'
 assert a['head']=='__ROOT_FROZEN_HEAD__' and a['tree']=='__ROOT_FROZEN_TREE__'
 assert images['state']=='BOTH_HOSTS_PACKAGED_NOT_RUNTIME' and images['build_proof_sha256']==a['build_sha256']
 assert all(x['head']==a['head'] and x['tree']==a['tree'] for x in (build,images))
 assert images['source_inventory_sha256']==build['source_inventory_sha256']
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
 assert hashlib.sha256(read(guard_path)).hexdigest()=='c88fa475e670912fe15e86509ac79a15b852b91bbbc17474ae735086e45408ba'
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
 receipt={'build_receipt':a['build'],'build_receipt_sha256':a['build_sha256'],'config_sha256':configs,'manifest_sha256':hashlib.sha256(read(root/'manifest.json')).hexdigest(),'source_head':a['head'],'source_tree':a['tree'],'image_receipt':a['images'],'plan':planned,'state':'PREPARATION_ONLY_NOT_BOOTSTRAP_ACCEPTANCE','pins_sha256':hashlib.sha256(read(args.pins)).hexdigest(),'launcher_sha256':LAUNCHER_SHA}
 (root/'plan-preparation.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt))
if __name__=='__main__':main()
