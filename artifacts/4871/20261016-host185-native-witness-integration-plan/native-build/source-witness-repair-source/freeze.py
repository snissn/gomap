import os,json,hashlib,tarfile,difflib,shutil
from pathlib import Path
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015'); N=D/'native';B=D/'native-build/source-witness-repair-source';OLD=D/'native-build/canonical-parent-repair-source'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def dump(p,x):p.write_text(json.dumps(x,indent=2,sort_keys=True)+'\n')
old=json.loads((B/'source-before.json').read_text());assert old==json.loads((OLD/'source-frozen.json').read_text())
paths=[]
for base,ds,fs in os.walk(N):
 ds[:]=[d for d in ds if d!='.git']
 for name in fs:
  p=Path(base,name);rel=str(p.relative_to(N))
  if rel!='.git' and p.is_file():paths.append(rel)
manifest={p:sha(N/p) for p in sorted(paths)};dump(B/'source-frozen.json',manifest)
changed=[p for p in sorted(set(old)|set(manifest)) if old.get(p)!=manifest.get(p)];assert len(changed)==7,changed
rows=[];patch=[]
with tarfile.open(OLD/'source-frozen.tar.gz','r:gz') as tf:
 for p in changed:
  prev=tf.extractfile(p).read() if p in old else b'';now=(N/p).read_bytes() if p in manifest else b''
  rows.append({'path':p,'before_sha256':old.get(p),'after_sha256':manifest.get(p),'bytes':len(now),'added':p not in old})
  patch.extend(difflib.unified_diff(prev.decode().splitlines(True),now.decode().splitlines(True),fromfile='a/'+p if p in old else '/dev/null',tofile='b/'+p))
  dst=B/'delta-files'/p;dst.parent.mkdir(parents=True,exist_ok=True);dst.write_bytes(now)
(B/'source.delta.patch').write_text(''.join(patch));dump(B/'changed-files.json',rows)
with tarfile.open(B/'source-frozen.tar.gz','w:gz') as tf:
 for p in manifest:tf.add(N/p,arcname=p,recursive=False)
with tarfile.open(B/'source-frozen.tar.gz','r:gz') as tf:assert {m.name:hashlib.sha256(tf.extractfile(m).read()).hexdigest() for m in tf if m.isfile()}==manifest
grant=D/'native-decision/root-source-witness-repair-selection.md';assert sha(grant)=='c83ce87ffcf7ff1bdf1b5d9838c52a59838fc4cebfcf3d54b429410616c9045a'
dump(B/'receipt.json',{'source_files':len(manifest),'source_manifest_sha256':sha(B/'source-frozen.json'),'source_archive_sha256':sha(B/'source-frozen.tar.gz'),'baseline_source_manifest_sha256':sha(OLD/'source-frozen.json'),'delta_patch_sha256':sha(B/'source.delta.patch'),'grant_sha256':sha(grant),'native_publication_runtime':False,'public_status':'Q32 RED/ACK0'})
oldcmd=json.loads((OLD/'proposed-commands.json').read_text());commands=[]
regex='^(TestNativeRootPreparationSourceWitnessV6|TestNativeRootPreparationSourceTerminalV6|TestNativePublishedRootPreparationDrainV6)$'
for name in ['compile-only','primitive-normal','primitive-race']:
 c=oldcmd['commands'][0].copy();c['env']=dict(c['env']);c['name']=name;c['source_only_selection_sha256']=sha(grant);c['env']['TMPDIR']=str(B/name/'tmp')
 c['argv']=[str(D/'toolchains/go1.26.3/bin/go'),'test']+(['-race'] if name=='primitive-race' else [])+['-p=1']+(['./TreeDB/db','./TreeDB/mvcc','./TreeDB/internal/primaryarena'] if name=='compile-only' else ['./TreeDB/db'])+['-run','^$' if name=='compile-only' else regex,'-count=1','-v','-timeout=3m'];commands.append(c)
dump(B/'proposed-commands.json',{'commands':commands,'source_manifest_sha256':sha(B/'source-frozen.json'),'actual33_executed':False})
s=(OLD/'run-verification.py').read_text().replace("B=D/'native-build/canonical-parent-repair-source'","B=D/'native-build/source-witness-repair-source'").replace('root-canonical-parent-repair-selection.md','root-source-witness-repair-selection.md').replace('da97231d275d2c45f904754c3e2f1b93e9122672fd7681fcb09f41fc6c8fe2da',sha(grant)).replace('assert len(expected)==10348','assert len(expected)=='+str(len(manifest)))
(B/'run-verification.py').write_text(s)
print(json.dumps({'source_files':len(manifest),'manifest_sha256':sha(B/'source-frozen.json'),'archive_sha256':sha(B/'source-frozen.tar.gz'),'changed':changed},indent=2))
