from pathlib import Path
import hashlib,json,os,shutil,subprocess
base=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013')
repo=base/'native-4878-traversal'
evidence=base/'evidence/root-primary-empty-fastpath-20261014'
parent='62253f2dc9a5e417e37c16e2dfae524daa70cbef'
prefix='artifacts/4878/20261014-primary-empty-fastpath'
packet=repo/prefix
source_paths=['TreeDB/tree/owned_iterator.go','TreeDB/tree/owned_root_empty_test.go','TreeDB/docs/spec/cow-cache-publication.md']
foreign=['TreeDB/internal/valuelog/native_zstd_rank_census_test.go','TreeDB/internal/valuelog/native_zstd_leaf_fixture_export_test.go']
def sha(b):return hashlib.sha256(b).hexdigest()
def git(*args,data=None):return subprocess.check_output(['git','-C',str(repo),*args],input=data).decode().strip()
assert git('rev-parse','HEAD')==parent
assert not git('diff','--cached','--name-only')
assert not packet.exists() and not (evidence/'publication-receipt.json').exists()
foreign_before={x:sha((repo/x).read_bytes()) for x in foreign}
allowed=set(source_paths+foreign)
actual=[line[3:] for line in subprocess.check_output(['git','-C',str(repo),'status','--porcelain=v1','-uall'],text=True).splitlines()]
assert set(actual)==allowed,actual
shutil.copytree(evidence,packet/'evidence')
for name in ['primary-empty-fastpath-focused-20261014.py','primary-empty-backimport-and-build-20261014.py','primary-empty-causal-comparison-20261014.py','publish-primary-fastpath-checkpoint-20261014.py']:
 target=packet/'scripts'/name;target.parent.mkdir(exist_ok=True);shutil.copyfile(base/name,target)
(packet/'README.md').write_text('''# Captured PRIMARY empty-root fast-path checkpoint

Production correction: a canonical zero-entry PRIMARY whose captured base is a digest-validated empty leaf now reaches the existing empty-backend COW read path. Nonempty/internal bases, any component and inline absence retain merge fallback; invalid directory/base errors propagate. No recapture, authority, ACK or persistent-pointer contract changes.

See evidence/causal-comparison/README.md and summary.json for two balanced same-host samples (16 diagnostic commands, 4.43 seconds invocation wall). Inline point/history/concurrent time fell41.33%/32.03%/36.49%; approximately58KB per read disappeared. Nine new tree cases plus four existing COW cases passed after a retained red baseline case. Independent Sol61 exact-source review found no blocker. Pointer point timing fell17.07%, but remains heavily affected by two cold lane creations and directory syncs; this is the next measured routing seam.

This is a focused implementation/evidence checkpoint, not merged production or full native/ordinary qualification, CI or final review acceptance. Performance projections are the actual coherent ordinary source retained in artifact854ce8c35d912783dea92faf26b058651071b85a plus the single shared source correction, not this native branch's whole production tree. The inventory proves that only owned_iterator.go differs between compared production projections. Additional identical endpoint-only lane reporting in both builds leaves the timed fixture/body unchanged. Original M0/M9 >10% relative Badger improvement in BOTH modes and all correctness/performance guards remain open and unchanged.
''')
files=[]
for p in sorted(packet.rglob('*')):
 if p.is_file():
  b=p.read_bytes();files.append({'path':str(p.relative_to(packet)),'bytes':len(b),'sha256':sha(b)})
manifest={'schema':'primary-empty-fastpath-v1','parent':parent,'source_paths':[{ 'path':x,'sha256':sha((repo/x).read_bytes())} for x in source_paths],'files':files,'bytes':sum(x['bytes'] for x in files)}
(packet/'artifact-manifest.json').write_text(json.dumps(manifest,indent=1)+'\n')
git('diff','--check','--',*source_paths)
git('add','-f','--',*source_paths,prefix)
staged=git('diff','--cached','--name-only').splitlines()
expected=set(source_paths+[prefix+'/'+x['path'] for x in files]+[prefix+'/artifact-manifest.json'])
assert set(staged)==expected,(set(staged)-expected,expected-set(staged))
# Retain raw Go/profile output byte-for-byte, including CPU banner whitespace.
git('diff','--cached','--check','--',*source_paths)
git('commit','-m','Reuse captured PRIMARY empty-base fast path for COW reads')
commit=git('rev-parse','HEAD')
assert git('rev-parse',commit+'^')==parent
assert set(git('diff-tree','--no-commit-id','--name-only','-r',parent,commit).splitlines())==expected
for x in source_paths:assert subprocess.check_output(['git','-C',str(repo),'show',commit+':'+x])==(repo/x).read_bytes()
for row in files:
 raw=subprocess.check_output(['git','-C',str(repo),'show',commit+':'+prefix+'/'+row['path']])
 assert sha(raw)==row['sha256'] and len(raw)==row['bytes']
assert foreign_before=={x:sha((repo/x).read_bytes()) for x in foreign}
assert set(line[3:] for line in subprocess.check_output(['git','-C',str(repo),'status','--porcelain=v1','-uall'],text=True).splitlines())==set(foreign)
ref=git('symbolic-ref','HEAD');assert ref=='refs/heads/codex/4878-native-traversal-20261014'
git('push','origin',ref+':'+ref)
assert git('ls-remote','origin',ref).split()[0]==commit
receipt={'commit':commit,'ref':ref,'parent':parent,'source_paths':source_paths,'members':len(files)+1,'bytes':manifest['bytes'],'manifest_sha256':sha((packet/'artifact-manifest.json').read_bytes()),'foreign_paths_preserved':foreign,'all_manifest_members_git_readback_verified':True,'url':f'https://github.com/snissn/gomap/tree/{commit}/{prefix}','qualification':'Focused checkpoint only, full graph gates open'}
(evidence/'publication-receipt.json').write_text(json.dumps(receipt,indent=1)+'\n')
print(json.dumps(receipt),flush=True)
