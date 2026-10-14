from pathlib import Path
import hashlib, json, shutil, subprocess

base = Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013')
repo = base/'native-4878-traversal'
evidence = base/'evidence/successor-4876-probe-20261014/prototype-v1'
prefix = 'artifacts/4876/20261014-captured-successor-prototype'
parent = 'ccca8d1f987130fbaeb343a89e4cea7e63b56efa'
foreign = ['TreeDB/internal/valuelog/native_zstd_rank_census_test.go', 'TreeDB/internal/valuelog/native_zstd_leaf_fixture_export_test.go']
def sha(b): return hashlib.sha256(b).hexdigest()
def git(*args): return subprocess.check_output(['git','-C',str(repo),*args],text=True).strip()
def status(): return subprocess.check_output(['git','-C',str(repo),'status','--porcelain=v1','-uall'],text=True).splitlines()
assert git('rev-parse','HEAD') == parent
assert git('symbolic-ref','HEAD') == 'refs/heads/codex/4878-native-traversal-20261014'
assert not git('diff','--cached','--name-only')
before = status()
assert set(x[3:] for x in before) == set(foreign)
foreign_sha = {x:sha((repo/x).read_bytes()) for x in foreign}
manifest_bytes = (evidence/'manifest.json').read_bytes()
assert sha(manifest_bytes) == '421b2e663ff36b662e3981b04a5118de9b106ba3411deee97b7350c4c361f427'
manifest = json.loads(manifest_bytes)
assert manifest['measured_successful_calls'] == 8 and manifest['failed_call'] == 'multilevel-inline-pair1-before'
assert manifest['no_more_collection'] and not manifest['disposable_only_release_list']
assert sha((evidence/'report.md').read_bytes()) == '1d3d1583a33728bbb026ca0174d87b1fba612aa5636f255dd6c22811ee302275'
rows = json.loads((evidence/'measurement-receipts.json').read_text())
measures = [r for r in rows if '-pair' in r['label']]
assert len(measures) == 9 and [r['exit'] for r in measures] == [0]*8+[1]
for r in rows:
    raw = Path(r['raw'])
    assert raw.parent == evidence and sha(raw.read_bytes()) == r['sha256']
packet = repo/prefix
assert not packet.exists()
packet.mkdir(parents=True)
retained_binaries = []
for item in manifest['publishable_artifact_allowlist']:
    rel = Path(item['path'])
    assert not rel.is_absolute() and '..' not in rel.parts
    src = evidence/rel
    assert src.is_file() and not src.is_symlink()
    b = src.read_bytes()
    assert len(b) == item['bytes'] and sha(b) == item['sha256']
    if rel.name.endswith('.test'):
        retained_binaries.append(item)
        continue
    dst = packet/'evidence'/rel
    dst.parent.mkdir(parents=True,exist_ok=True)
    dst.write_bytes(b)
assert len(retained_binaries) == 2
(packet/'evidence/manifest.json').write_bytes(manifest_bytes)
shutil.copyfile(evidence/'report.md',packet/'README.md')
(packet/'scripts').mkdir()
shutil.copyfile(base/'publish-successor-prototype-20261014.py',packet/'scripts/publish-successor-prototype-20261014.py')
files=[]
for p in sorted(packet.rglob('*')):
    if p.is_file():
        b=p.read_bytes();files.append({'path':str(p.relative_to(packet)),'bytes':len(b),'sha256':sha(b)})
out = {'schema':'mvcc-captured-successor-prototype-v1','parent':parent,'disposition':'NO_GO_FOR_PRODUCTION_ACTIVATION','production_binding':'Source copies and complete censuses bind isolated coherent imported runtime; artifact commit is not compiled native runtime. Six changed production files remain nonmergeable prototype evidence.','successful_calls':8,'failed_baseline_calls':1,'multilevel_candidate_calls':0,'timed_unit':'public CommitAt20 plus GetAt15 returning persisted version10','process_elapsed_seconds_including_failed_call':sum(r['elapsed_s'] for r in measures),'retained_local_binaries':retained_binaries,'cleanup_release':[],'files':files,'bytes':sum(x['bytes'] for x in files)}
(packet/'artifact-manifest.json').write_text(json.dumps(out,indent=1)+'\n')
git('add','-f','--',prefix)
expected = {prefix+'/'+x['path'] for x in files}|{prefix+'/artifact-manifest.json'}
assert set(git('diff','--cached','--name-only').splitlines()) == expected
git('commit','-m','Preserve stopped captured-successor public performance prototype')
commit=git('rev-parse','HEAD')
assert git('rev-parse',commit+'^') == parent
assert set(git('diff-tree','--no-commit-id','--name-only','-r',parent,commit).splitlines()) == expected
for item in files:
    b=subprocess.check_output(['git','-C',str(repo),'show',commit+':'+prefix+'/'+item['path']])
    assert len(b)==item['bytes'] and sha(b)==item['sha256']
assert subprocess.check_output(['git','-C',str(repo),'show',commit+':'+prefix+'/artifact-manifest.json']) == (packet/'artifact-manifest.json').read_bytes()
assert status()==before and {x:sha((repo/x).read_bytes()) for x in foreign} == foreign_sha
ref=git('symbolic-ref','HEAD')
git('push','origin',ref+':'+ref)
assert git('ls-remote','origin',ref).split()[0] == commit
result={'commit':commit,'parent':parent,'ref':ref,'url':f'https://github.com/snissn/gomap/tree/{commit}/{prefix}','members':len(files)+1,'bytes':out['bytes'],'manifest_sha256':sha((packet/'artifact-manifest.json').read_bytes()),'readback_all_members':True,'foreign_preserved':True,'disposition':out['disposition'],'retained_local_binary_count':2,'released_paths':[]}
(evidence/'publication-receipt.json').write_text(json.dumps(result,indent=1)+'\n')
print(json.dumps(result),flush=True)
