from pathlib import Path
import datetime,hashlib,json,os,re,tarfile
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');O=D/'native-main-integration/index-pin-controls';W=D/'native-integrated'
def h(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def archive(p):
 out={}
 with tarfile.open(p,'r:gz') as tf:
  for m in tf:
   if m.isfile():
    assert m.name not in out
    out[m.name]=hashlib.sha256(tf.extractfile(m).read()).hexdigest()
 return out
manifest=json.loads((O/'evidence-manifest.json').read_text())
for n,v in manifest.items():assert h(O/n)==v,n
assert h(O/'evidence.tar.gz')=='24cbb133f49962de535a4754781a201c848a9bd1310fe247e5fd15fe744db127'
payload=archive(O/'evidence.tar.gz');assert payload==dict(manifest,**{'evidence-manifest.json':h(O/'evidence-manifest.json')})
assert h(O/'selection.json')=='7cfa832751e74b960c1f619334938d81480d097a61229b7afafb9f66121fd3e2'
g=json.loads((O/'selection.json').read_text());sp=Path(g['source_manifest']);sa=Path(g['source_archive']);frozen=json.loads(sp.read_text())
assert h(sp)==g['source_manifest_sha256'] and h(sa)==g['source_archive_sha256'] and archive(sa)==frozen and len(frozen)==10377
live={}
for base,ds,fs in os.walk(W):
 ds[:]=[d for d in ds if d!='.git']
 for n in fs:
  p=Path(base,n);rel=str(p.relative_to(W))
  if rel!='.git' and p.is_file():live[rel]=h(p)
assert live==frozen
results=[];totals={False:[0,0],True:[0,0]}
for c in g['commands']:
 call=O/c['id'];actual=json.loads((call/'command.json').read_text());r=json.loads((call/'result.json').read_text());raw=(call/'run.log').read_text()
 assert actual['argv']==c['argv'] and actual['env']==c['env'] and actual['cwd']==str(W) and actual['executed'] is True and c['executed'] is False
 assert actual['outer_timeout_seconds']==c['outer_timeout_seconds'] and actual['actual_pid']==r['pid'] and actual['actual_pgid']==r['pgid']
 assert h(call/'source-before.json')==h(call/'source-after.json')==h(sp)
 for name,pattern in [('RUN',r'^=== RUN\s+(\S+)'),('PASS',r'^\s*--- PASS: (\S+)'),('FAIL',r'^\s*--- FAIL: (\S+)'),('SKIP',r'^\s*--- SKIP: (\S+)')]:assert re.findall(pattern,raw,re.M)==r['test_'+name+'_names']
 expected=list(c['top_level_tests'])+[p+'/'+n for p,children in g['required_child_tests'].items() if p in c['top_level_tests'] for n in children]
 assert set(r['test_RUN_names'])==set(expected)==set(r['test_PASS_names']) and len(r['test_RUN_names'])==len(expected)==len(r['test_PASS_names'])
 assert r['exit_code']==0 and r['status']=='PASS' and not r['test_SKIP_names'] and not r['test_FAIL_names'] and r['leader_joined'] and r['group_and_children_empty'] and not r['signals'] and not r['timeout'] and not r['stop_reasons']
 assert r['raw_log_sha256']==h(call/'run.log')
 totals[c['race']][0]+=r['top_level_pass_count'];totals[c['race']][1]+=len(r['test_PASS_names']);results.append(r)
assert totals=={False:[7,38],True:[7,38]}
i=json.loads((O/'identity.json').read_text());idx=json.loads((O/'index-binding.json').read_text());after=json.loads((O/'index-after.json').read_text())
assert i['runner_sha256']==h(O/'run-controls.py') and i['go_sha256']==h(Path(i['go_binary'])) and i['compiler_sha256']==h(D/'toolchains/go1.26.3/pkg/tool/linux_amd64/compile')
assert after['unchanged'] and after['index_sha256_after']==idx['index_sha256_before']==h(Path(idx['index_path']))
assert not Path('/proc',str(i['runner_pid'])).exists()
pgids={r['pgid'] for r in results}
for p in Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:s=(p/'stat').read_text();v=s[s.rfind(')')+2:].split()
 except OSError:continue
 assert int(v[2]) not in pgids and int(v[1])!=i['runner_pid']
s=json.loads((O/'summary.json').read_text());assert s['commands_executed']==4 and not s['commands_not_executed'] and s['all_authorized_pass'] and not s['actual33_executed'] and not s['native_publication_executed']
receipt=dict(verified_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),verifier_sha256=h(Path(__file__)),source_files=len(frozen),source_manifest_sha256=h(sp),source_archive_sha256=h(sa),evidence_members=len(payload),evidence_manifest_sha256=h(O/'evidence-manifest.json'),evidence_archive_sha256=h(O/'evidence.tar.gz'),source_and_index_unchanged=True,processes_joined_and_absent=True,execution_result='PASS fourcalls; eachmode7tops31children',results=[{k:r[k] for k in ['id','status','elapsed_seconds','pid','pgid','top_level_pass_count']} for r in results],runtime_repeated=False,public_status='Q32 RED/ACK0; no actual33 successor/native publication/allowance')
(D/'native-main-integration/index-pin-controls-root-verification.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt,indent=2))
