from pathlib import Path
import hashlib, json, shutil, subprocess

base = Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013')
repo = base/'native-4878-traversal'
evidence = base/'sprint-performance-20261014/evidence/populated-backend-sol61-v1'
prefix = 'artifacts/4871/20261014-populated-backend-performance'
parent = '8457698ca964486c39467eb6447986c84a6d5b48'
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
assert sha((evidence/'final-receipt.json').read_bytes()) == 'b54ba2d8069c375ca789422c0ceb8d87d665306b86d62743d388b2beb5a5350a'
receipt = json.loads((evidence/'final-receipt.json').read_text())
assert receipt['all_12_exit_zero'] and receipt['all_12_raw_sha_verified'] and receipt['all_source_and_binary_preservation_verified']
for name,h in receipt['artifacts'].items(): assert sha((evidence/name).read_bytes()) == h
packet = repo/prefix
assert not packet.exists()
shutil.copytree(evidence, packet/'evidence')
shutil.copyfile(evidence/'report.md', packet/'README.md')
(packet/'scripts').mkdir()
shutil.copyfile(base/'publish-populated-performance-20261014.py', packet/'scripts/publish-populated-performance-20261014.py')
files = []
for p in sorted(packet.rglob('*')):
    if p.is_file():
        b=p.read_bytes(); files.append({'path':str(p.relative_to(packet)),'bytes':len(b),'sha256':sha(b)})
manifest = {'schema':'mvcc-populated-public-performance-v1','parent':parent,'diagnostic_only':True,'production_binding':'Exact coherent ordinary production projections and public harness censuses in evidence/build-manifest.json; this artifact commit is not the compiled native whole tree','invocations':12,'measured_seconds':4.295062443707138,'timed_unit':'One public CommitAt(version20) plus GetAt(timestamp15) returning persisted version10','files':files,'bytes':sum(x['bytes'] for x in files)}
(packet/'artifact-manifest.json').write_text(json.dumps(manifest,indent=1)+'\n')
git('add','-f','--',prefix)
expected=set(prefix+'/'+x['path'] for x in files)|{prefix+'/artifact-manifest.json'}
assert set(git('diff','--cached','--name-only').splitlines()) == expected
git('commit','-m','Preserve bounded populated MVCC public-path performance diagnosis')
commit=git('rev-parse','HEAD')
assert git('rev-parse',commit+'^') == parent
assert set(git('diff-tree','--no-commit-id','--name-only','-r',parent,commit).splitlines()) == expected
for x in files:
    b=subprocess.check_output(['git','-C',str(repo),'show',commit+':'+prefix+'/'+x['path']]); assert len(b)==x['bytes'] and sha(b)==x['sha256']
assert subprocess.check_output(['git','-C',str(repo),'show',commit+':'+prefix+'/artifact-manifest.json']) == (packet/'artifact-manifest.json').read_bytes()
assert status()==before and {x:sha((repo/x).read_bytes()) for x in foreign} == foreign_sha
ref=git('symbolic-ref','HEAD')
git('push','origin',ref+':'+ref)
assert git('ls-remote','origin',ref).split()[0] == commit
result={'commit':commit,'parent':parent,'ref':ref,'url':f'https://github.com/snissn/gomap/tree/{commit}/{prefix}','members':len(files)+1,'bytes':manifest['bytes'],'manifest_sha256':sha((packet/'artifact-manifest.json').read_bytes()),'readback_all_members':True,'foreign_preserved':True,'qualification':'Diagnostic artifact only. No M5 activation or full parent qualification.'}
(evidence/'publication-receipt.json').write_text(json.dumps(result,indent=1)+'\n')
print(json.dumps(result),flush=True)
