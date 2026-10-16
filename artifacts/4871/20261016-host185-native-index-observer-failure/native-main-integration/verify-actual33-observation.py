from pathlib import Path
import datetime, hashlib, json, os, re, tarfile
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015'); O=D/'native-main-integration/actual33-observation'; W=D/'native-integrated'
def h(p): return hashlib.sha256(p.read_bytes()).hexdigest()
manifest=json.loads((O/'evidence-manifest.json').read_text())
for n,v in manifest.items(): assert h(O/n)==v,n
assert h(O/'evidence.tar.gz')=='1cb3c15e7d5844ba2ccecf62ae345fcc97d59e8413db8820d18d53ff2842bc1e'
with tarfile.open(O/'evidence.tar.gz','r:gz') as tf:
 payload={}
 for m in tf:
  if m.isfile():
   assert m.name not in payload
   payload[m.name]=hashlib.sha256(tf.extractfile(m).read()).hexdigest()
assert payload==dict(manifest,**{'evidence-manifest.json':h(O/'evidence-manifest.json')})
grant=json.loads((O/'grant-original.json').read_text()); expected_command=grant['command']
assert h(O/'grant-original.json')=='8ed1d4596b0d2f83d80a281105c5386664276fcba47833decb9ef2faa3281859'
source_path=Path(expected_command['source_manifest']); frozen=json.loads(source_path.read_text()); source_archive=Path(expected_command['source_archive'])
assert h(source_path)==expected_command['source_manifest_sha256'] and h(source_archive)==expected_command['source_archive_sha256']
with tarfile.open(source_archive,'r:gz') as tf:
 source_payload={}
 for m in tf:
  if m.isfile():
   assert m.name not in source_payload
   source_payload[m.name]=hashlib.sha256(tf.extractfile(m).read()).hexdigest()
assert source_payload==frozen and len(frozen)==10375
live={}
for base,ds,fs in os.walk(W):
 ds[:]=[d for d in ds if d!='.git']
 for name in fs:
  p=Path(base,name); rel=str(p.relative_to(W))
  if rel!='.git' and p.is_file():live[rel]=h(p)
assert live==frozen
call=O/expected_command['name']; actual=json.loads((call/'command.json').read_text()); r=json.loads((call/'result.json').read_text()); raw=(call/'run.log').read_text()
assert actual['argv']==expected_command['argv'] and actual['env']==expected_command['env'] and actual['cwd']==str(W)
assert actual['outer_timeout_seconds']==240 and actual['executed'] is False
assert h(call/'source-before.json')==h(call/'source-after.json')==h(source_path)
for label,pat in [('RUN',r'^=== RUN\s+(\S+)'),('PASS',r'^\s*--- PASS: (\S+)'),('FAIL',r'^\s*--- FAIL: (\S+)'),('SKIP',r'^\s*--- SKIP: (\S+)')]: assert re.findall(pat,raw,re.M)==r['test_'+label+'_names']
assert r['exit_code']==1 and r['status']=='STOP' and r['top_level_pass_count']==0 and len(r['test_RUN_names'])==1 and len(r['test_FAIL_names'])==1
assert not r['test_SKIP_names'] and r['leader_joined'] and r['group_and_children_empty'] and not r['signals'] and not r['timeout']
assert r['raw_log_sha256']==h(call/'run.log')
identity=json.loads((O/'identity.json').read_text()); index=json.loads((O/'index-binding.json').read_text()); after=json.loads((O/'index-after.json').read_text())
assert after['unchanged'] and after['index_sha256_after']==index['index_sha256_before']==h(Path(index['index_path']))
assert identity['runner_sha256']==h(O/'run-observation.py')
assert identity['go_sha256']==h(Path(identity['go_binary'])) and identity['compiler_sha256']==h(D/'toolchains/go1.26.3/pkg/tool/linux_amd64/compile')
assert not Path('/proc',str(identity['runner_pid'])).exists()
for p in Path('/proc').iterdir():
 if not p.name.isdigit(): continue
 try:
  s=(p/'stat').read_text(); v=s[s.rfind(')')+2:].split()
 except (OSError,ValueError): continue
 assert int(v[2])!=r['pgid'] and int(v[1])!=identity['runner_pid']
summary=json.loads((O/'summary.json').read_text())
assert summary['commands_executed']==1 and not summary['all_authorized_pass'] and summary['actual33_executed'] and not summary['native_publication_executed']
receipt={'verified_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'verifier_sha256':h(Path(__file__)),'source_files':len(frozen),'source_manifest_sha256':h(source_path),'source_archive_sha256':h(source_archive),'evidence_archive_sha256':h(O/'evidence.tar.gz'),'evidence_archive_members':len(payload),'evidence_manifest_sha256':h(O/'evidence-manifest.json'),'execution_result':'FAIL at group4 index0; group4 remainder/group5/later writer-cancel-reopen assertions not reached','diagnosis':'not inferred by verifier','command_metadata_note':'executed:false is copied from the proposal template. Result, process and raw log prove the single actual execution; template is not an execution-state assertion. Raw files preserved.','runtime_repeated':False,'source_and_index_unchanged':True,'processes_joined_and_absent':True,'public_status':'Q32 RED/ACK0; no native publication or allowance'}
(D/'native-main-integration/actual33-root-verification.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt,indent=2))
