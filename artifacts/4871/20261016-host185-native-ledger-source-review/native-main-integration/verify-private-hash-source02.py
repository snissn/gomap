import datetime, difflib, hashlib, json, subprocess, tarfile
from pathlib import Path

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
B = D / 'native-main-integration/private-hash-capacity/source-final02'
P = D / 'native-main-integration/private-hash-capacity/compile-source01'
A = D / 'native-main-integration/private-hash-source02-root-read'
W = D / 'native-integrated'
sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
read = lambda p: json.loads(p.read_text())
r = read(B/'freeze-receipt.json')
m = read(B/'source-frozen.json')
assert sha(B/'source-frozen.json') == r['source_manifest_sha256'] == '15f0548d03b03923cc8785816b21ee957df3a4f114a2578cba0ef3bbbdd142ec'
assert sha(B/'source-frozen.tar.gz') == r['source_archive_sha256'] == '7fd459782bcd9bea535b91956324c424b2a8703a102a71526917202130b6eacc'
assert sha(B/'source-delta.patch') == r['source_delta_sha256'] == '252fb0ffed9207bd9c26903a9def1ea722f7b81cc03f19776162a6e9d8268818'
assert sha(B/'changed-paths.json') == r['changed_paths_sha256']
assert len(m) == r['source_files'] == 10389
base_path = D/'native-main-integration/index-pin-inventory-final/source-final.json'
assert sha(base_path) == r['baseline_manifest_sha256'] == '23e37a210f156838de8adecbf8532c0cd16cd12f6d6c37fb05529b7974d0869f'
base = read(base_path)
changed = sorted(n for n in base.keys() | m.keys() if base.get(n) != m.get(n))
rows = read(B/'changed-paths.json')
assert [x['path'] for x in rows] == changed and len(changed) == r['changed_paths'] == 30
for row in rows:
    assert row['baseline_sha256'] == base.get(row['path'])
    assert row['final_sha256'] == m[row['path']]
assert not (base.keys() - m.keys())
assert len(m.keys() - base.keys()) == r['added_paths'] == 12
assert sha(B/'changed-source.tar.gz') == r['changed_source_archive_sha256']
def archive(path):
    result = {}
    with tarfile.open(path, 'r:gz') as tf:
        for item in tf:
            assert item.isfile() and item.name not in result and not item.name.startswith('/') and '..' not in Path(item.name).parts
            result[item.name] = tf.extractfile(item).read()
    return result
full = archive(B/'source-frozen.tar.gz')
assert {n:hashlib.sha256(v).hexdigest() for n,v in full.items()} == m
small = archive(B/'changed-source.tar.gz')
assert small == {n:full[n] for n in changed}
assert all(sha(W/n) == h for n,h in m.items())
for name,h in r['selections'].items(): assert sha(D/'native-decision'/name) == h
assert sha(P/'source-frozen.json') == 'ad721ad3112f1d1348b80f5c71d2c9711b9e9456dfde1811e23cdb3ab8afafbe'
prior = read(P/'source-frozen.json')
prior_bytes = archive(P/'source-frozen.tar.gz')
assert {n:hashlib.sha256(v).hexdigest() for n,v in prior_bytes.items()} == prior
increment = sorted(n for n in prior.keys() | m.keys() if prior.get(n) != m.get(n))
A.mkdir(exist_ok=False)
for n in changed:
    target = A/n
    target.parent.mkdir(parents=True,exist_ok=True)
    target.write_bytes(full[n])
patch = ''.join(''.join(difflib.unified_diff(prior_bytes.get(n,b'').decode().splitlines(True),full.get(n,b'').decode().splitlines(True),fromfile='ad721/'+n,tofile='15f054/'+n)) for n in increment)
(A/'delta-from-ad721.patch').write_text(patch)
(A/'incremental-paths.json').write_text(json.dumps(increment,indent=2)+'\n')
index = subprocess.check_output(['git','write-tree'],cwd=W,text=True).strip()
assert index == '52ca5ef561f6d27928356ca2f836e722be7c4b31'
out = dict(verified_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),source_manifest_sha256=r['source_manifest_sha256'],source_archive_sha256=r['source_archive_sha256'],source_files=len(m),baseline_paths=changed,incremental_paths_from_ad721=increment,full_archive_verified=True,changed_archive_verified=True,live_source_verified=True,index_unchanged=index,root_extraction=str(A),delta_from_ad721_sha256=sha(A/'delta-from-ad721.patch'),compile_not_yet_asserted=True,test_bodies=0)
(D/'native-main-integration/private-hash-source02-root-verification.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps(out,indent=2))
