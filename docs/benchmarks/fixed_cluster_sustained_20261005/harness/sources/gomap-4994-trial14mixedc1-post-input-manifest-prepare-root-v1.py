"""Local-only, one-shot promotion after actual V2 probe artifact acceptance.
Never launches a process. Writes a new input bundle/archive and an INACTIVE manifest.
Root alone supplies actual proof/review pins, reviews this source, and later activates
five flags using separate real permission and product/harness-mode evidence.
"""
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import struct
import tarfile

if not __debug__:
    raise RuntimeError('ordinary Python assertion checks required; not -O')
V = Path('/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1')
PRE = V/'pre-inputs-root-v1'
OUT = V/'inputs-root-v1'
ARCHIVE = V/'inputs-root-v1.tar.gz'
MANIFEST = V/'mixed-approved-root-v1.json'
PROMOTION_PROOF = V/'post-input-manifest-preparation-root-v1.json'
PROBE = V/'growth-query-root-v2/stdout.jsonl'
PROOF = V/'growth-artifact-root-proof-v2.json'
LIFECYCLE = V/'growth-lifecycle-root-v2'
COLLECTOR = Path('/tmp/gomap-4994-mixed-window-collector-root-v5.py')
# Fill only from actual successful V2 artifacts and independent artifact review.
ROOT_PROBE_PROOF_SHA256 = 'ec7c47ef57b72a8fc40b1c3e7b9dab8ca3e2f17629ddf3e15cb6afb4a7aeb5ab'
ARTIFACT_REVIEW_PATH = '/tmp/gomap-4994-trial14mixedc1-growth-independent-artifact-review-root-v1.json'
ARTIFACT_REVIEW_SHA256 = '6bf3fcbbf748bd59a4c555842fd0825c5992503c62674d53616a8afd1a0cb0b7'
ARTIFACT_ACCEPT_FIELD = 'decision'
ARTIFACT_ACCEPT_VALUE = 'ACCEPT'
# The independent review must pin stdout, root proof, lifecycle result and all4
# actual final-inspect wrappers. Root selects its actual raw path/hash map field.
ARTIFACT_PIN_FIELD = 'artifact_pins'
FINAL_INSPECT_WRAPPERS = {'node-a': {'path': '/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1/growth-lifecycle-root-v2/022-inspect-node-a.json', 'sha256': 'a679340d4c617c5d304257fc227f0a14a87002f39b0601d8a11239bd50b0eccf'}, 'node-b': {'path': '/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1/growth-lifecycle-root-v2/025-inspect-node-b.json', 'sha256': '2a3af5838c22d690a47b45c8efbefa692fa226b174f52711116f48716786e543'}, 'node-c': {'path': '/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1/growth-lifecycle-root-v2/028-inspect-node-c.json', 'sha256': 'ccaecbf870b5db6299e71c4bd4f7cf9304cbd1b402e0cdb2e5496c589cc5e56e'}, 'node-d': {'path': '/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1/growth-lifecycle-root-v2/031-inspect-node-d.json', 'sha256': '809cc729a87eb315f9d669287915f116e445240624261b3750d1b1608cebc24d'}}
DRIVER_UID_GID = '1000:1000'  # proposed actual nonroot account; no permission claim
COLLECTOR_SHA256 = 'c5ca596190ed59977f097e6f8e183462d31de27b408a04c133e3726587fa11e8'  # exact inert V5 source; separate activation review required
HEAD = 'b625421c06ae6f9398c384686f3c50ce2389ac21'
TREE = '223df16137b4d11565a4a5ed033973c40ffa7556'
GO_SHA = '7ec5f0ed4f36ca69e355abc8c8c0d44920808bdce1c864482544a462b8453e04'
DRIVER_SHA = '701c02e0688ac0e923ceaff239c764ee2678fc993a12db086ce5deec7677744a'
SERVER_SHA = '64877e9d0163bed36494442c1c1799150babc21f54fc322e760e70c08fd8427f'
PRE_INV_SHA = '6da0f7a1755708f7dcf64b25717110a733ea462ded2192223a762826aa691674'
BOOT_SHA = '4c29bc8d618fc59f96dd0e7c4bf4f87223777af91756e9c7bf1140ae1c7e4685'
CONFIG_SHA = '7b439d781cd329629a426697450cac4ee7b016bf2a972f587fac570e30322f53'
PLAN_SHA = 'a3910b818e7a94ce5e1fe3cc2181132fbd18bb17d8429f5b9af8e068aa45c1d2'
BOOT_REVIEW = Path('/tmp/gomap-4994-trial14mixedc1-bootstrap-independent-artifact-review-root-v1.json')
BOOT_REVIEW_SHA = '222ae6fd79ca43bbdb0d6ef64ed0e82c4383d6e0dfec28df3383553b41f18439'
LANDED = V/'landed-source-root-v1.json'
LANDED_SHA = '4721d75e16c99fb16f3d13f06fb03dd75a79d2ca608f8413336ece51ff5aa21a'
BUILD = V/'binding-receipts-root-v1/build-for-collector.json'
BUILD_SHA = '447ff2ab404cb455f7589fe36fb3619df5dd27eb19d33469ff257183264b51a6'
SOURCE_REVIEW = V/'binding-receipts-root-v1/source-prereview.json'
SOURCE_REVIEW_SHA = '2c9b6e24ab9cca255ff4d36bc3b989bf3b60bafa74192c6518b0d1e854701da3'
SOURCE_INV = Path('/tmp/gomap-4994-v7-final-slot-runtime-build-root-v1/git-source-inventory.json')
CIDS = {'node-a':'ba61c08f4cbd60ac656c4701c79e5c2b2d5bd4a633ec6d7c093acac3af8da12a',
        'node-b':'cab138126f9e8a9a46daffca5a53c1a828144f9b19b85e42593e68817a81afd4',
        'node-c':'af2c3f89b590a547fa388e49cc8250571b55dd16405898d341706931d745678a',
        'node-d':'6e1968565621d2b2457a4c917b7af73748b39a0f7d5922b4c36ec7ad65f95fa5'}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def read(path, expected=None):
    path = Path(path)
    assert path.is_file() and not path.is_symlink() and path.stat().st_size <= 64<<20, path
    raw = path.read_bytes()
    if expected is not None:
        assert isinstance(expected,str) and re.fullmatch('[0-9a-f]{64}',expected)
        assert sha(raw) == expected, path
    return raw


def encoded(value):
    return (json.dumps(value,indent=2)+'\n').encode()


def main():
    # Refuse before import, mkdir, archive, manifest or any output mutation.
    assert isinstance(ROOT_PROBE_PROOF_SHA256,str) and re.fullmatch('[0-9a-f]{64}',ROOT_PROBE_PROOF_SHA256), 'actual accepted V2 probe pins required'
    assert isinstance(ARTIFACT_REVIEW_PATH,str) and isinstance(ARTIFACT_REVIEW_SHA256,str)
    assert ARTIFACT_ACCEPT_FIELD in ('decision','outcome') and isinstance(ARTIFACT_ACCEPT_VALUE,str) and ARTIFACT_ACCEPT_VALUE.startswith('ACCEPT')
    assert isinstance(ARTIFACT_PIN_FIELD,str) and ARTIFACT_PIN_FIELD
    assert isinstance(FINAL_INSPECT_WRAPPERS,dict) and set(FINAL_INSPECT_WRAPPERS)==set(CIDS)
    assert isinstance(DRIVER_UID_GID,str) and re.fullmatch('[1-9][0-9]*:[1-9][0-9]*',DRIVER_UID_GID)
    assert isinstance(COLLECTOR_SHA256,str) and re.fullmatch('[0-9a-f]{64}',COLLECTOR_SHA256)
    read(COLLECTOR,COLLECTOR_SHA256)
    # V5's import is inert. Reuse strict JSON, path/ownership and local validators.
    spec=importlib.util.spec_from_file_location('mixed_collector_v5',COLLECTOR)
    c=importlib.util.module_from_spec(spec);spec.loader.exec_module(c)
    load=lambda p,h=None:c.strict_json(read(p,h))
    independent=load(ARTIFACT_REVIEW_PATH,ARTIFACT_REVIEW_SHA256)
    assert independent[ARTIFACT_ACCEPT_FIELD]==ARTIFACT_ACCEPT_VALUE and independent['findings']==[]
    acceptance_pins=independent[ARTIFACT_PIN_FIELD]
    assert isinstance(acceptance_pins,dict)
    proof=load(PROOF,ROOT_PROBE_PROOF_SHA256)
    raw=read(PROBE,proof['stdout_sha256'])
    assert acceptance_pins[str(PROOF)]==ROOT_PROBE_PROOF_SHA256 and acceptance_pins[str(PROBE)]==sha(raw)
    assert proof['status']=='PASS_ROOT_ASSERTIONS' and proof['findings']==[]
    assert proof['counts']==dict(Planned=70,Attempted=70,Succeeded=70,Failed=0,Canceled=0,Unknown=0,Unissued=0)
    assert proof['ordinary_fresh_ids']==1 and proof['mutation_attempts']==2 and proof['native_searches']==68 and proof['post_population_rows']==10005
    assert proof['cleanup']['state']=='PASS_STOPPED' and proof['cleanup']['stores_preserved'] is True
    assert proof['cleanup']['exit_code']==0 and proof['cleanup']['oom_killed'] is False
    assert proof['pre_input_inventory_sha256']==PRE_INV_SHA and proof['bootstrap_review_sha256']==BOOT_REVIEW_SHA
    lifecycle=load(LIFECYCLE/'result.json',acceptance_pins[str(LIFECYCLE/'result.json')])
    assert lifecycle==dict(errors=[],state='PASS_STOPPED',stores_preserved=True)
    br=load(BOOT_REVIEW,BOOT_REVIEW_SHA)
    assert br['outcome']=='ACCEPT_BOOTSTRAP_AND_SEALED_PRE_INPUT_ARTIFACTS' and br['findings']==[]
    assert br['artifact_pins'][str(PRE/'input-inventory.json')]==PRE_INV_SHA
    landed=load(LANDED,LANDED_SHA)
    assert landed['state']=='LANDED_SOURCE_TREE_VERIFIED' and landed['candidate_head']==HEAD and landed['candidate_tree']==landed['landed_tree']==TREE and landed['exact_tree_equality'] is True
    build=load(BUILD,BUILD_SHA);source=load(SOURCE_REVIEW,SOURCE_REVIEW_SHA)
    assert build['head']==source['candidate_head']==HEAD and build['tree']==source['candidate_tree']==TREE
    assert build['source_inventory_sha256']==GO_SHA and build['driver_sha256']==DRIVER_SHA and build['server_sha256']==SERVER_SHA
    assert source['outcome']=='ACCEPT'
    identities=proof['identities']
    for k,v in dict(source_head=HEAD,source_tree=TREE,source_inventory_sha256=GO_SHA,driver_sha256=DRIVER_SHA,server_sha256=SERVER_SHA,bootstrap_sha256=BOOT_SHA,config_sha256=CONFIG_SHA,plan_sha256=PLAN_SHA).items():
        assert identities[k]==v,k
    inv=load(PRE/'input-inventory.json',PRE_INV_SHA)
    assert len(inv)==74
    files={}
    for name,h in inv.items():
        c.input_name(name);files[name]=read(PRE/name,h)
    boot=c.strict_json(files['bootstrap.json']);assert sha(files['bootstrap.json'])==BOOT_SHA and sha(files['config.json'])==CONFIG_SHA
    plan_raw=read(V/'bootstrap-root-v1/plan.json',PLAN_SHA);plan=c.strict_json(plan_raw)
    assert [n['node'] for n in plan['nodes']]==list(CIDS) and plan['binary_sha256']==SERVER_SHA
    events=[c.strict_json(line) for line in raw.splitlines() if line.strip()]
    planned=[e['Report'] for e in events if e.get('Event')=='planned'];results=[e['Report'] for e in events if e.get('Event')=='result']
    assert len(planned)==len(results)==1
    probe=results[0]
    assert probe['RunID']==proof['run_id']=='rf4trial14mixedc1query01' and probe['BinarySHA256']==DRIVER_SHA and probe['ConfigSHA256']==CONFIG_SHA and probe['BootstrapSHA256']==BOOT_SHA
    assert len(probe['Operations'])==len(planned[0]['Operations'])==70 and probe['Counts']==proof['counts']
    assert all(o['Outcome']=='succeeded' for o in probe['Operations'])
    writes=[o for o in probe['Operations'] if o['Kind']=='insert']
    assert len(writes)==2 and writes[0]['Phase']=='concurrent' and writes[1]['Phase']=='explicit-retry'
    assert writes[0]['InsertResponse']['CommitIndex']<writes[1]['InsertResponse']['CommitIndex']==proof['highest_commit_index']==probe['HighestCommitIndex']
    c.validate_bootstrap_prefix(boot,proof['highest_commit_index'])
    # Recompute baseline from retained FP32 corpus and the actual single original.
    vectors={'doc-%06d'%i:v for i,v in enumerate(struct.iter_unpack('<128f',files['dataset/documents.f32']))}
    assert len(vectors)==10000
    for name,x,y in [('seed-x',1,0),('seed-minus-x',-1,0),('seed-minus-y',0,-1),(boot['Insert']['VisibleID'],0,1)]:
        assert name not in vectors;vectors[name]=[x,y]+[0]*126
    req=writes[0]['InsertRequest'];id=base64.b64decode(req['ID'],validate=True).decode()
    assert id==writes[0]['InsertResponse']['VisibleID'] and id not in vectors
    vectors[id]=req['Vector'];assert len(vectors)==10005
    digest=hashlib.sha256()
    for name in sorted(vectors):
        b=name.encode();digest.update(struct.pack('<I',len(b))+b+struct.pack('<128f',*vectors[name]))
    assert digest.hexdigest()==proof['post_population_sha256']
    baseline=dict(PopulationRows=10005,PopulationSHA256=digest.hexdigest(),HighestCommitIndex=proof['highest_commit_index'],CorpusRows=10000,AnchorRows=5,Dimensions=128,Queries=16)
    nodes=[]
    for n in plan['nodes']:
        meta=FINAL_INSPECT_WRAPPERS[n['node']]
        assert set(meta)=={'path','sha256'} and Path(meta['path']).parent==LIFECYCLE
        assert Path(meta['path']).name.endswith('-inspect-'+n['node']+'.json')
        finals=sorted(LIFECYCLE.glob('*-inspect-'+n['node']+'.json'))
        assert finals and Path(meta['path'])==finals[-1], 'actual last V2 lifecycle inspect required'
        assert acceptance_pins[meta['path']]==meta['sha256']
        wrapper=load(meta['path'],meta['sha256'])
        assert wrapper['exit']==0 and not wrapper.get('timed_out',False)
        argv=wrapper['argv'];assert argv[0]=='ssh' and 'mikers@'+n['host'] in argv and argv[-1]=='docker inspect '+n['name']
        direct=wrapper['stdout'].encode();array=c.strict_json(direct)
        assert isinstance(array,list) and len(array)==1
        owned=dict(n,cid=CIDS[n['node']]);x=c.validate_voter(array[0],owned)
        assert not x['State']['Running'] and x['State']['ExitCode']==0
        stopped=next(z for z in proof['cleanup']['voters'] if z['node']==n['node'])
        assert stopped['cid']==x['Id'] and stopped['exit_code']==0 and stopped['oom_killed'] is False
        name='probe-final-inspect-'+n['node']+'.json';files[name]=direct
        nodes.append(dict(node=n['node'],host=n['host'],image=n['image'],server_sha256=SERVER_SHA,cid=x['Id'],inspect_path=str(OUT/name),inspect_sha256=sha(direct)))
    provenance=c.strict_json(files['provenance.json'])
    assert provenance['Phase']=='pre' and provenance['RuntimeSourceHead']==HEAD and provenance['ServerBinarySHA256']==SERVER_SHA
    assert all(provenance[k] is True for k in ('RootAccepted','InitializationSucceeded','CleanReopenSucceeded','ExclusiveWriterStopped'))
    provenance.update(Phase='post-only',ProbeSHA256=sha(raw),ProbeBinarySHA256=DRIVER_SHA,ProbeRunID=probe['RunID'],RootAccepted=True,ExclusiveWriterStopped=True)
    # RootAccepted here refers only to the actually reviewed bootstrap+probe inputs.
    # Every mixed execution flag below remains false, including RootAccepted.
    files['provenance.json']=encoded(provenance);files['probe.jsonl']=raw
    files['plan.json']=plan_raw;files['baseline.json']=encoded(baseline)
    files['probe-root-proof.json']=read(PROOF,ROOT_PROBE_PROOF_SHA256)
    files['probe-independent-artifact-review.json']=read(ARTIFACT_REVIEW_PATH,ARTIFACT_REVIEW_SHA256)
    files['probe-lifecycle-result.json']=read(LIFECYCLE/'result.json',acceptance_pins[str(LIFECYCLE/'result.json')])
    new_inv={name:sha(value) for name,value in sorted(files.items())}
    inventory_raw=encoded(new_inv)
    receipts=dict(source_inventory=str(SOURCE_INV),build=str(BUILD),source_prereview=str(SOURCE_REVIEW),bootstrap=str(OUT/'bootstrap.json'),config=str(OUT/'config.json'),plan=str(OUT/'plan.json'),baseline=str(OUT/'baseline.json'))
    pins={str(SOURCE_INV):GO_SHA,str(BUILD):BUILD_SHA,str(SOURCE_REVIEW):SOURCE_REVIEW_SHA,str(LANDED):LANDED_SHA,str(BOOT_REVIEW):BOOT_REVIEW_SHA,str(OUT/'input-inventory.json'):sha(inventory_raw)}
    # Root activation must add the actual permission proof and its eight raw
    # receipt paths/hashes to local_pins, then use real mode/permission gates.
    # This inactive assembler never sets those five flags or fabricates receipts.
    pins.update({str(OUT/name):sha(files[name]) for name in ['bootstrap.json','config.json','plan.json','baseline.json']+[Path(n['inspect_path']).name for n in nodes]})
    manifest=dict(Version=1,RunID=c.RUN,remote_root=c.ROOT,source_head=HEAD,source_tree=TREE,source_inventory_sha256=GO_SHA,collector_sha256=COLLECTOR_SHA256,query_image=build['driver_image'],driver_sha256=DRIVER_SHA,driver_uid_gid=DRIVER_UID_GID,bootstrap_sha256=BOOT_SHA,config_sha256=CONFIG_SHA,plan_sha256=PLAN_SHA,input_inventory_sha256=sha(inventory_raw),baseline=baseline,nodes=nodes,receipts=receipts,local_pins=pins)
    manifest.update({flag:False for flag in c.FLAGS})
    keys=set(c.FLAGS)|{'Version','RunID','remote_root','source_head','source_tree','source_inventory_sha256','collector_sha256','query_image','driver_sha256','driver_uid_gid','bootstrap_sha256','config_sha256','plan_sha256','input_inventory_sha256','baseline','nodes','receipts','local_pins'}
    assert set(manifest)==keys and all(manifest[k] is False for k in c.FLAGS)
    assert set(baseline)=={'PopulationRows','PopulationSHA256','HighestCommitIndex','CorpusRows','AnchorRows','Dimensions','Queries'}
    c.validate_source_bindings(manifest,{path:read(path,h) for path,h in pins.items() if not path.startswith(str(OUT)+'/')})
    for target in [OUT,ARCHIVE,MANIFEST,PROMOTION_PROOF]:
        assert not target.exists() and not target.is_symlink(), target
    OUT.mkdir()
    for name,value in files.items():
        target=OUT/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(value)
    (OUT/'input-inventory.json').write_bytes(inventory_raw)
    c.prepare_local(manifest)  # actual local semantic checks; flags remain false
    with tarfile.open(ARCHIVE,'x:gz') as archive:
        for path in sorted(OUT.rglob('*')):
            if path.is_file():archive.add(path,arcname=str(path.relative_to(OUT)),recursive=False)
    MANIFEST.write_bytes(encoded(manifest))
    promotion=dict(state='PREPARED_LOCAL_INPUTS_INACTIVE_MANIFEST',manifest_sha256=sha(read(MANIFEST)),archive_sha256=sha(read(ARCHIVE)),input_inventory_sha256=sha(inventory_raw),baseline=baseline,root_probe_proof_sha256=ROOT_PROBE_PROOF_SHA256,artifact_review_sha256=ARTIFACT_REVIEW_SHA256,all_execution_flags_false=True,source_head=HEAD,source_tree=TREE,landed_source_receipt_sha256=LANDED_SHA,ownership='root alone may activate using separate actual product/harness/permission receipts; this assembler starts no process',limits=['No pre-recall, mixed-window, capacity, throughput or whole-lifetime resource acceptance','Copied original candidate/provisional packets are unchanged; post-only provenance and direct inspect files are new','One-shot outputs preserved even on assembly failure; no deletion or overwrite/retry'])
    PROMOTION_PROOF.write_bytes(encoded(promotion));print(json.dumps(promotion))


if __name__=='__main__':
    main()
