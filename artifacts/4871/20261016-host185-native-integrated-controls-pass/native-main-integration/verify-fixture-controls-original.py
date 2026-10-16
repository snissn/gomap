from pathlib import Path
import json,hashlib,tarfile,re,datetime
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');O=D/'native-main-integration/fixture-controls'
def h(p):return hashlib.sha256(p.read_bytes()).hexdigest()
manifest=json.loads((O/'evidence-manifest.json').read_text())
for n,r in manifest.items():assert h(O/n)==r['sha256'] and (O/n).stat().st_size==r['bytes'],n
archive=O/'evidence.tar.gz'
with tarfile.open(archive,'r:gz') as tf:
 payload={}
 for m in tf:
  if m.isfile():
   assert m.name not in payload;payload[m.name]=hashlib.sha256(tf.extractfile(m).read()).hexdigest()
assert set(payload)==set(manifest)|{'evidence-manifest.json'}
assert all(payload[n]==r['sha256'] for n,r in manifest.items())
assert payload['evidence-manifest.json']==h(O/'evidence-manifest.json')
g=json.loads((O/'selection.json').read_text());summary=json.loads((O/'summary.json').read_text());assert summary['commands_executed']==18 and summary['all_authorized_pass'] and summary['all_groups_empty'] and summary['source_still_frozen']
assert h(O/'selection.json')=='6001cea5d41861593d87d8da9378e93238c2757525ae6aa56e6848b0d171b9dc'
results=[];top={False:0,True:0};allnames={False:0,True:0}
for c in g['commands']:
 p=O/c['id'];actual=json.loads((p/'command.json').read_text());r=json.loads((p/'result.json').read_text());raw=(p/'run.log').read_text()
 assert actual['argv']==c['argv'] and actual['selector']==c['selector'] and actual['top_level_tests']==c['top_level_tests']
 assert h(p/'source-before.json')==h(p/'source-after.json')==g['source_manifest_sha256']
 for label,pat in [('RUN',r'^=== RUN\s+(\S+)'),('PASS',r'^\s*--- PASS: (\S+)'),('FAIL',r'^\s*--- FAIL: (\S+)'),('SKIP',r'^\s*--- SKIP: (\S+)')]:assert re.findall(pat,raw,re.M)==r['test_'+label+'_names']
 assert r['status']=='PASS' and r['exit_code']==0 and r['leader_joined'] and r['group_and_children_empty'] and not r['signals'] and not r['timeout']
 assert r['raw_log_sha256']==h(p/'run.log') and not r['stop_reasons'] and not r['test_SKIP_names'] and not r['test_FAIL_names']
 assert set(r['top_level_RUN_names'])==set(c['top_level_tests']) and all(n in r['test_PASS_names'] for n in r['test_RUN_names'])
 assert all(n in r['test_RUN_names'] and n in r['test_PASS_names'] for n in r['required_child_names'])
 if c.get('required_leaf'):assert r['M4_selected_leaf_names']==[c['required_leaf']]
 top[c['race']]+=r['top_level_pass_count'];allnames[c['race']]+=len(r['test_PASS_names'])
 results.append({k:r[k] for k in ['id','status','elapsed_seconds','pid','pgid','top_level_pass_count']})
assert top=={False:19,True:63}
index=json.loads((O/'index-binding.json').read_text());after=json.loads((O/'index-after.json').read_text());assert after['unchanged'] and after['index_sha256_after']==index['index_sha256_before']==h(Path(index['index_path']))
identity=json.loads((O/'identity.json').read_text());assert identity['go_sha256']=='d68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65' and identity['compiler_sha256']=='3bb421d581f60b9ab48c2c73adf5c018557e89af32ad018d9c4417afa4bf90c1'
receipt={'verified_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'evidence_manifest_sha256':h(O/'evidence-manifest.json'),'archive_sha256':h(archive),'archive_payloads':len(payload),'results':results,'normal_top':top[False],'normal_all':allnames[False],'race_top':top[True],'race_all':allnames[True],'prior_normal_passes_reused':47,'prior_source_manifest':'e434542a59d55cf67c90d6a16c5787abeb908efda4c1c705033658b9ba8e0577','all_source_and_process_receipts_verified':True,'runtime_repeated':False,'actual33_executed':False,'native_publication_executed':False}
(D/'native-main-integration/fixture-controls-root-verification.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt,indent=2))
