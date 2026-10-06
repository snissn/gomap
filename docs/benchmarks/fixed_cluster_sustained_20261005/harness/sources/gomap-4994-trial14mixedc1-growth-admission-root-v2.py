# Fail closed before any subprocess, SSH, or artifact-directory creation.
if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import hashlib,json,pathlib
ADMISSION_PATH = '/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1/growth-admission-root-v2.json'
ADMISSION_SHA256 = '3f3202dc7260b4d4676f020e1d1066c238128fab16585174d6d3c2a760cd7872'
BOOTSTRAP_REVIEW_PATH = '/tmp/gomap-4994-trial14mixedc1-bootstrap-independent-artifact-review-root-v1.json'
BOOTSTRAP_REVIEW_SHA256 = '222ae6fd79ca43bbdb0d6ef64ed0e82c4383d6e0dfec28df3383553b41f18439'
LANDED_RECEIPT_PATH = '/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1/landed-source-root-v1.json'
LANDED_RECEIPT_SHA256 = '4721d75e16c99fb16f3d13f06fb03dd75a79d2ca608f8413336ece51ff5aa21a'
SOURCE_PREREVIEW_PATH = '/tmp/gomap-4994-trial14mixedc1-growth-source-independent-prereview-root-v2.json'
SOURCE_PREREVIEW_SHA256 = '2581a8475f297df021ed8d3b8eced76c8d346804225e234ec47aad81df4f6c09'
EXPECTED = {'remote_root': '/home/mikers/gomap-4250-twohost-rf4trial14mixedc1', 'query_image': 'sha256:718a79a455321acc6409436db4de44414ed8c4dd42e5ca684e9abf0a664889f8', 'driver_sha256': '701c02e0688ac0e923ceaff239c764ee2678fc993a12db086ce5deec7677744a', 'server_sha256': '64877e9d0163bed36494442c1c1799150babc21f54fc322e760e70c08fd8427f', 'bootstrap_sha256': '4c29bc8d618fc59f96dd0e7c4bf4f87223777af91756e9c7bf1140ae1c7e4685', 'config_sha256': '7b439d781cd329629a426697450cac4ee7b016bf2a972f587fac570e30322f53', 'plan_sha256': 'a3910b818e7a94ce5e1fe3cc2181132fbd18bb17d8429f5b9af8e068aa45c1d2', 'source_head': 'b625421c06ae6f9398c384686f3c50ce2389ac21', 'source_tree': '223df16137b4d11565a4a5ed033973c40ffa7556', 'source_inventory_sha256': '7ec5f0ed4f36ca69e355abc8c8c0d44920808bdce1c864482544a462b8453e04', 'root_precollection_receipt_sha256': None}
INPUT = pathlib.Path('/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1/pre-inputs-root-v1')
def sha(p): return hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
def pinned(path, digest):
 assert isinstance(path,str) and path and isinstance(digest,str) and len(digest)==64, 'root must fill actual admission pins'
 assert sha(path)==digest, path
 return json.loads(pathlib.Path(path).read_bytes())
def admit():
 a=pinned(ADMISSION_PATH,ADMISSION_SHA256)
 review=pinned(BOOTSTRAP_REVIEW_PATH,BOOTSTRAP_REVIEW_SHA256)
 landed=pinned(LANDED_RECEIPT_PATH,LANDED_RECEIPT_SHA256)
 source=pinned(SOURCE_PREREVIEW_PATH,SOURCE_PREREVIEW_SHA256)
 product=pinned('/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1/binding-receipts-root-v1/source-prereview.json','2c9b6e24ab9cca255ff4d36bc3b989bf3b60bafa74192c6518b0d1e854701da3')
 build=pinned('/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1/binding-receipts-root-v1/build.json','c31d6cb04c845616618c37d9d70d3af4b322b2b926cadcb1cfb98c37c53ed7ed')
 assert product['outcome']=='ACCEPT' and product['candidate_head']==EXPECTED['source_head'] and product['candidate_tree']==EXPECTED['source_tree']
 assert build['state']=='VERIFIED_BUILD_FROM_FROZEN_SOURCE' and build['source_verified_before_after'] is True
 assert build['head']==EXPECTED['source_head'] and build['tree']==EXPECTED['source_tree'] and build['source_inventory_sha256']==EXPECTED['source_inventory_sha256']
 assert build['driver_sha256']==EXPECTED['driver_sha256'] and build['server_sha256']==EXPECTED['server_sha256'] and build['driver_image']==EXPECTED['query_image']
 assert build['server_images']=={'node-a':'sha256:5071d652bdb56e152ca9cdba581e88b00bf513e1b17ed9e3439e0cfb8ffcf9a5','node-b':'sha256:5071d652bdb56e152ca9cdba581e88b00bf513e1b17ed9e3439e0cfb8ffcf9a5','node-c':EXPECTED['query_image'],'node-d':EXPECTED['query_image']}

 assert review['outcome']=='ACCEPT_BOOTSTRAP_AND_SEALED_PRE_INPUT_ARTIFACTS' and not review['findings']
 assert source['decision']=='ACCEPT' and not source['findings']
 # These fields are a required root receipt contract, not an invented accepted receipt.
 assert a['state']=='APPROVED_ONE_BOUNDED_FUNCTIONAL_PROBE' and a['campaign']=='rf4trial14mixedc1'
 assert a['bootstrap_review_sha256']==BOOTSTRAP_REVIEW_SHA256 and a['landed_source_receipt_sha256']==LANDED_RECEIPT_SHA256 and a['source_prereview_sha256']==SOURCE_PREREVIEW_SHA256
 assert landed['state']=='LANDED_SOURCE_TREE_VERIFIED' and landed['candidate_tree']==landed['landed_tree']==EXPECTED['source_tree'] and landed['exact_tree_equality'] is True
 assert landed['candidate_head']==EXPECTED['source_head'] and landed['pr']==4995 and landed['all_checks']=='SUCCESS'
 assert isinstance(landed['merge_commit'],str) and len(landed['merge_commit'])==40
 assert a['landed_head']==landed['merge_commit'] and a['landed_tree']==EXPECTED['source_tree']
 assert a['candidate_head']==EXPECTED['source_head'] and a['go_inventory_sha256']==EXPECTED['source_inventory_sha256']
 assert a['fresh_inserts']==1 and a['operations']==70 and a['baseline_rows']==10005
 assert a['input_inventory_sha256']==sha(INPUT/'input-inventory.json')=='6da0f7a1755708f7dcf64b25717110a733ea462ded2192223a762826aa691674'
 inventory=json.loads((INPUT/'input-inventory.json').read_bytes())
 for name,h in inventory.items():
  p=pathlib.PurePosixPath(name); assert not p.is_absolute() and '..' not in p.parts
  assert sha(INPUT/name)==h, name
 chain=json.loads((INPUT/'root-chain-proof.json').read_bytes())
 assert chain['status']=='PASS_ROOT_FULL_BOOTSTRAP_CHAIN' and chain['pre_live_rows']==10004 and chain['qualification_prefix']==88
 assert chain['voters']==[{'node': 'node-a', 'container_id': 'ba61c08f4cbd60ac656c4701c79e5c2b2d5bd4a633ec6d7c093acac3af8da12a'}, {'node': 'node-b', 'container_id': 'cab138126f9e8a9a46daffca5a53c1a828144f9b19b85e42593e68817a81afd4'}, {'node': 'node-c', 'container_id': 'af2c3f89b590a547fa388e49cc8250571b55dd16405898d341706931d745678a'}, {'node': 'node-d', 'container_id': '6e1968565621d2b2457a4c917b7af73748b39a0f7d5922b4c36ec7ad65f95fa5'}]
 for key in ('bootstrap_sha256','config_sha256','plan_sha256','driver_sha256','server_sha256'):
  assert a[key]==EXPECTED[key],key
 assert a['collector_sha256']==sha('/tmp/gomap-4994-trial14mixedc1-growth-query-root-v2.py') and a['lifecycle_sha256']==sha('/tmp/gomap-4994-trial14mixedc1-growth-lifecycle-root-v2.py')
 assert a['exact_validator_sha256']==sha('/tmp/gomap-4994-trial14mixedc1-growth-exact-validate-root-v2.py') and a['artifact_validator_sha256']==sha('/tmp/gomap-4994-trial14mixedc1-growth-artifact-verify-root-v2.py')
 out=dict(EXPECTED);out['root_precollection_receipt_sha256']=ADMISSION_SHA256
 return out
