from pathlib import Path
import collections, datetime, hashlib, json, re, subprocess, tarfile
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015'); W=D/'native-integrated'; B=D/'native-build/ordinary-integrated-source'; A=D/'native-main-integration'
def h(b):return hashlib.sha256(b).hexdigest()
def fh(p):
 s=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):s.update(b)
 return s.hexdigest()
def dump(p,o):p.write_text(json.dumps(o,indent=2,sort_keys=True)+'\n')
assert fh(B/'source-frozen.json')=='56f77963dffe17198ce428f27f635f44d9b946183939a1b108a044d40a34009e'
assert fh(B/'source-frozen.tar.gz')=='422da27116a0a21315b149e4972e131c738f74628d6172734979f13c8ea2e2f6'
I=json.loads((B/'source-frozen.json').read_text()); O=json.loads((D/'ordinary-main-integration/source-final.json').read_text()); N=json.loads((D/'native-build/source-witness-repair-source/source-frozen.json').read_text())
seen={}
with tarfile.open(B/'source-frozen.tar.gz','r:gz') as tf:
 for m in tf:
  assert m.isfile() and m.name not in seen,m.name
  seen[m.name]=h(tf.extractfile(m).read())
assert seen==I
assert all(fh(W/n)==v for n,v in I.items())
actual=set(subprocess.check_output(['git','ls-files','-z','--cached','--others','--exclude-standard'],cwd=W).decode().split('\0'))-{''}
assert actual==set(I)
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=W,text=True).strip()=='0149d78dba2574b09d7119d2631ec78a144e088d'
prior=json.loads((A/'witness-input-census.json').read_text())['rows']; declared=json.loads((B/'integration-census.json').read_text()); assert len(prior)==len(declared)==len(I)==10372
counts=collections.Counter(); rows=[]
for p,r in zip(prior,declared):
 assert {k:r[k] for k in p}==p
 n=p['path'];v=I[n]; status=p['status'];counts[status]+=1
 assert r['integrated_sha256']==v and r['ordinary_preserved']==(v==O.get(n)) and r['native_preserved']==(v==N.get(n))
 if status in ('identical','ordinary-owned'):assert v==O[n]
 rows.append(r)
changed=[r for r in rows if r['integrated_sha256']!=r['ordinary_sha256']];adapted=[r for r in rows if r['status']=='native-owned' and not r['native_preserved']]
assert changed==json.loads((B/'changed-files.json').read_text()) and len(changed)==197
assert adapted==json.loads((B/'native-owned-adaptations.json').read_text()) and len(adapted)==6
assert not set(O)-set(I) and len(set(I)-set(O))==114
for r in changed:assert fh(B/'delta-files'/r['path'])==r['integrated_sha256']
selected=[r for r in rows if r['status'] in ('native-owned','divergent')]
for r in selected:assert fh(B/'selected-action-files'/r['path'])==r['integrated_sha256']
selected_names={r['path'] for r in selected}
sections=re.split(r'(?=^--- (?:a/|/dev/null))',(B/'witness-to-integrated.delta.patch').read_text(),flags=re.M)
focused=[]
for section in sections:
 if not section.strip():continue
 n=section.splitlines()[1].removeprefix('+++ b/')
 if n in selected_names:focused.append(section)
(A/'witness-selected-composition.patch').write_text(''.join(focused))
summary=dict(at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),manifest_sha256=fh(B/'source-frozen.json'),archive_sha256=fh(B/'source-frozen.tar.gz'),archive_files_verified=len(I),live_files_verified=len(I),counts=dict(counts),selected_actions=len(selected),actual_changed=len(changed),additions=114,modifications=83,deletions=0,native_adaptations=[r['path'] for r in adapted],focused_delta_bytes=(A/'witness-selected-composition.patch').stat().st_size,source_mutations=False,runtime_executed=False)
dump(A/'combined-input-verification.json',summary);print(json.dumps(summary,indent=2))
