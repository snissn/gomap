from pathlib import Path
import collections, datetime, hashlib, json, subprocess, tarfile
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015'); W=D/'native-integrated'; B=D/'native-build/source-witness-repair-source'; A=D/'native-main-integration'
def h(b):return hashlib.sha256(b).hexdigest()
def fh(p):
 s=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):s.update(b)
 return s.hexdigest()
def dump(p,o):p.write_text(json.dumps(o,indent=2,sort_keys=True)+'\n')
N=json.loads((B/'source-frozen.json').read_text()); old=json.loads((D/'native-build/canonical-parent-repair-source/source-frozen.json').read_text()); O=json.loads((D/'ordinary-main-integration/source-final.json').read_text())
assert fh(B/'source-frozen.json')=='4b230aa27b6a51d2394d2cd237403f956ac4897afb4b76e9e2e47bfb39947fd2'
assert fh(B/'source-frozen.tar.gz')=='44682995595a636fe99ab3febcebca1802f8992574eb4fab8dbef49f355ea03c'
seen={}
with tarfile.open(B/'source-frozen.tar.gz','r:gz') as tf:
 for m in tf:
  assert m.isfile() and m.name not in seen,m.name
  seen[m.name]=h(tf.extractfile(m).read())
assert seen==N
assert all(fh(D/'native'/n)==v for n,v in N.items())
delta=sorted(n for n in set(old)|set(N) if old.get(n)!=N.get(n)); declared=json.loads((B/'changed-files.json').read_text())
assert delta==sorted(x['path'] for x in declared) and len(delta)==7
for x in declared:
 assert old.get(x['path'])==x['before_sha256'] and N[x['path']]==x['after_sha256']
 assert fh(B/'delta-files'/x['path'])==x['after_sha256']
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=W,text=True).strip(); status=subprocess.check_output(['git','status','--porcelain'],cwd=W,text=True)
assert head=='0149d78dba2574b09d7119d2631ec78a144e088d' and not status
tracked=set(subprocess.check_output(['git','ls-files','-z'],cwd=W).decode().split('\0'))-{''}
assert tracked==set(O)
assert all(fh(W/n)==v for n,v in O.items())
census=json.loads((D/'native-decision/ordinary-native-integration-extraction/census.json').read_text()); C={r['path']:r['common_sha256'] for r in census['rows'] if r['common_sha256'] is not None}
rows=[]; counts=collections.Counter()
for n in sorted(set(C)|set(O)|set(N)):
 c,o,a=C.get(n),O.get(n),N.get(n)
 status='identical' if o==a else 'native-owned' if o==c else 'ordinary-owned' if a==c else 'divergent'
 counts[status]+=1;rows.append(dict(path=n,status=status,common_sha256=c,ordinary_sha256=o,native_sha256=a,new_relative_common=c is None))
assert dict(counts)=={'ordinary-owned':108,'identical':10061,'divergent':22,'native-owned':181},dict(counts)
prior=json.loads((D/'native-decision/ordinary-native-integration-plan.json').read_text())['path_classification']
assert [r['path'] for r in rows if r['status']=='divergent']==prior['divergent']
assert set(delta)<=set(r['path'] for r in rows if r['status']=='native-owned')
selected=[r for r in rows if r['status'] in ('native-owned','divergent')]
assert len(selected)==203 and sum(r['ordinary_sha256'] is None for r in selected)==114
assert not any(r['native_sha256'] is None for r in selected)
dump(A/'witness-input-census.json',dict(common=census['common'],counts=dict(counts),rows=rows,selected_actions=len(selected),additions=114,modifications=89,deletions=0))
summary=dict(at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),source_manifest_sha256=fh(B/'source-frozen.json'),archive_sha256=fh(B/'source-frozen.tar.gz'),archive_files_verified=len(N),live_native_files_verified=len(N),delta_paths=delta,ordinary_head=head,ordinary_clean=True,ordinary_files_verified=len(O),counts=dict(counts),union_paths=len(rows),selected_actions=len(selected),additions=114,modifications=89,deletions=0,census_sha256=fh(A/'witness-input-census.json'),source_mutations=False,runtime_executed=False)
dump(A/'witness-input-binding.json',summary);print(json.dumps(summary,indent=2))
