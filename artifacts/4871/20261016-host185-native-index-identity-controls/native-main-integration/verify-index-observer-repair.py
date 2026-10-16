from pathlib import Path
import datetime,hashlib,json,os,re,tarfile
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015'); W=D/'native-integrated'; B=D/'native-main-integration/index-observer-repair'
def h(p):return hashlib.sha256(p.read_bytes()).hexdigest()
assert h(B/'source-frozen.json')=='9d6c8de347d5c64c75cbb4d3ae9d0a067d5a9283c7967d466c88633cff06fef9'
assert h(B/'source-frozen.tar.gz')=='72eb1c83025494d45be6450db3c274f8224a97277bdad90aaf6fe9fe9750a6bc'
source=json.loads((B/'source-frozen.json').read_text()); prior=json.loads((D/'native-main-integration/fixture-inventory-final/source-final.json').read_text())
with tarfile.open(B/'source-frozen.tar.gz','r:gz') as tf:
 archive={}
 for m in tf:
  if m.isfile():
   assert m.name not in archive
   archive[m.name]=hashlib.sha256(tf.extractfile(m).read()).hexdigest()
assert archive==source and len(source)==10377
live={}
for base,ds,fs in os.walk(W):
 ds[:]=[x for x in ds if x!='.git']
 for f in fs:
  p=Path(base,f);rel=str(p.relative_to(W))
  if rel!='.git' and p.is_file():live[rel]=h(p)
assert live==source and set(prior)<=set(source)
changed={n for n,v in source.items() if prior.get(n)!=v}; rows=json.loads((B/'changed-files.json').read_text())
assert len(changed)==6 and changed=={r['path'] for r in rows}
for r in rows:assert prior.get(r['path'])==r['before_sha256'] and source[r['path']]==r['after_sha256']==h(B/'delta-files'/r['path'])
assert len(set(source)-set(prior))==2 and h(B/'index-observer-repair.delta.patch')=='41c66dceefdf2f48eda29031f3be7a51bcbe5818c17f993d407e70ec194e24aa'
O=B/'verification'; C=O/'compile-only'; r=json.loads((C/'result.json').read_text()); command=json.loads((C/'command.json').read_text()); identity=json.loads((O/'identity.json').read_text()); raw=(C/'run.log').read_text()
assert command['argv']==[str(D/'toolchains/go1.26.3/bin/go'),'test','-p=1','-run','^$','-count=1','-timeout=3m','./TreeDB/db','./TreeDB/internal/rootpublication','./TreeDB/pager','./TreeDB/mvcc']
assert command['grant_sha256']=='7c9941e990ba64cfd413ec66ffd2bf649d6fe829c599f20052d33d940f07fdfe' and command['executed'] is True
assert h(C/'source-before.json')==h(C/'source-after.json')==h(B/'source-frozen.json')
packages=re.findall(r'^ok\s+github.com/snissn/gomap/(\S+)\s+.*\[no tests to run\]$',raw,re.M)
assert packages==['TreeDB/db','TreeDB/internal/rootpublication','TreeDB/pager','TreeDB/mvcc']
assert '=== RUN' not in raw and r['test_RUN_count']==0 and r['exit_code']==0 and r['leader_joined'] and r['group_and_children_empty'] and r['subreaper_enabled']
assert not r['signals'] and not r['timeout'] and not r['residual_observed_before_cleanup']
assert identity['go_binary_sha256']==h(Path(identity['go_binary'])) and identity['compile_binary_sha256']==h(D/'toolchains/go1.26.3/pkg/tool/linux_amd64/compile')
assert not Path('/proc',str(identity['runner_pid'])).exists()
for p in Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:s=(p/'stat').read_text();v=s[s.rfind(')')+2:].split()
 except OSError:continue
 assert int(v[2])!=r['pgid'] and int(v[1])!=identity['runner_pid']
x={'verified_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'verifier_sha256':h(Path(__file__)),'source_manifest_sha256':h(B/'source-frozen.json'),'source_archive_sha256':h(B/'source-frozen.tar.gz'),'source_files':len(source),'exact_delta_paths':sorted(changed),'compile_packages':packages,'compile_elapsed_seconds':r['elapsed_seconds'],'test_bodies':0,'source_before_after_unchanged':True,'compiler_and_process_join_verified':True,'stale_metadata_note':'command.source_only_selection_sha256 copied from old54c32af1 origin proposal. Actual grant_sha256 and runner assert7c9941; exact authorized argv/source are verified. Original record retained.','further_runtime_executed':False,'native_publication':False}
(D/'native-main-integration/index-observer-root-verification.json').write_text(json.dumps(x,indent=2)+'\n');print(json.dumps(x,indent=2))
