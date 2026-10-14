from pathlib import Path
import hashlib,json,os,shutil,subprocess,time
base=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013')
native=base/'native-4878-traversal';ordinary=base/'ordinary-5111'
out=base/'evidence/root-primary-empty-fastpath-20261014'
perf=base/'sprint-performance-20261014'
go=base/'toolchains/go1.26.3-from-185-20261014'
def sha(b):return hashlib.sha256(b).hexdigest()
source='TreeDB/tree/owned_iterator.go';test='TreeDB/tree/owned_root_empty_test.go';doc='TreeDB/docs/spec/cow-cache-publication.md'
assert not (out/'backimport-receipt.json').exists()
assert (ordinary/source).read_bytes()==(out/'owned_iterator.before.go').read_bytes()
assert not (ordinary/test).exists()
oldDoc=(native/doc).read_bytes();assert (ordinary/doc).read_bytes()==oldDoc
anchor='Point lookup, successor selection, merge traversal, pointer/checksum decoding\nand copying occur after the cut latch is released.'
new='''The empty-backend shortcut is a proof about the captured root. It accepts an
empty leaf directly, or a canonical PRIMARY directory with no components whose
digest-validated base is an empty leaf. Nonempty/internal bases and any component
(including an inline absence) retain the same-cut merge path; physical validation
errors propagate. It never consults a newer published root to infer emptiness.

'''+anchor
assert oldDoc.decode().count(anchor)==1
(native/doc).write_text(oldDoc.decode().replace(anchor,new))
allowed={source,test,doc}
def census(root):
 return {str(p.relative_to(root)):sha(p.read_bytes()) for p in root.rglob('*') if p.is_file() and not p.is_symlink() and '.git' not in p.relative_to(root).parts and '.evidence' not in p.relative_to(root).parts and str(p.relative_to(root)) not in allowed}
outside=census(ordinary)
for name in [source,test,doc]:
 p=ordinary/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes((native/name).read_bytes())
assert census(ordinary)==outside
receipt={'paths':[{'path':x,'sha256':sha((ordinary/x).read_bytes())} for x in [source,test,doc]],'outside_paths_preserved':len(outside),'new_root_capture':False,'authority_change':False}
(out/'backimport-receipt.json').write_text(json.dumps(receipt,indent=1)+'\n')
print(json.dumps(receipt),flush=True)
env=dict(os.environ,GOROOT=str(go),GOWORK='off',GOTOOLCHAIN='local',CGO_ENABLED='1',GOMAXPROCS='4')
argv=[str(go/'bin/go'),'test','./TreeDB/tree','./TreeDB/caching','-run','^(TestOwnedUserRootEmptyCapturedPrimary|TestCOWEmptyBasisIteratorNeedsNoReadWorkspace|TestCOWEmptyShardIteratorDiskTombstoneAndOldCut|TestCOWPointerImmediatePointAndIteratorOldCut|TestCOWPointerCheckpointCapturedBasisAndTombstones)$','-count=1','-timeout=120s','-v']
log=out/'affected-cow-focus.log';started=time.time()
with log.open('wb') as f:r=subprocess.run(argv,cwd=ordinary,env=env,stdout=f,stderr=subprocess.STDOUT,timeout=180)
(out/'affected-cow-focus.receipt.json').write_text(json.dumps({'argv':argv,'exit':r.returncode,'seconds':time.time()-started,'sha256':sha(log.read_bytes())},indent=1)+'\n')
print(json.dumps({'affected_focus_exit':r.returncode,'raw_sha256':sha(log.read_bytes())}),flush=True)
if r.returncode:print(log.read_text()[-6000:]);raise SystemExit(r.returncode)
target=perf/'sources/fastpath-candidate';assert not target.exists()
shutil.copytree(perf/'sources/coherent-candidate',target,copy_function=os.link)
p=target/source;p.unlink();p.write_bytes((ordinary/source).read_bytes())
previous=json.loads((perf/'evidence/coherent-candidate-projection.json').read_text())
rows=previous['production_files'];matches=0
for row in rows:
 raw=(target/row['path']).read_bytes()
 assert raw==(ordinary/row['path']).read_bytes(),row['path']
 if row['path']==source:row.update(sha256=sha(raw),bytes=len(raw));matches+=1
 else:assert sha(raw)==row['sha256'],row['path']
assert matches==1
(out/'fastpath-projection.json').write_text(json.dumps({'source':'coherent-candidate exact retained projection plus single shared emptiness correction','production_files':rows,'production_bytes':sum(x['bytes'] for x in rows),'previous_artifact':'854ce8c35d912783dea92faf26b058651071b85a','qualification':'Diagnostic-only coherent actual ordinary source'},indent=1)+'\n')
binary=perf/'binaries/fastpath-candidate-mvcc.test'
argv=[str(go/'bin/go'),'test','-c','-trimpath','-buildvcs=false','-o',str(binary),'./TreeDB/mvcc/sprintperf']
log=out/'build.log';started=time.time()
with log.open('wb') as f:r=subprocess.run(argv,cwd=target,env=env,stdout=f,stderr=subprocess.STDOUT,timeout=180)
receipt={'argv':argv,'exit':r.returncode,'seconds':time.time()-started,'log_sha256':sha(log.read_bytes()),'projection_sha256':sha((out/'fastpath-projection.json').read_bytes()),'harness':[{ 'path':str(p.relative_to(target)),'sha256':sha(p.read_bytes())} for p in sorted((target/'TreeDB/mvcc/sprintperf').glob('*.go'))]}
if r.returncode==0:receipt['binary_sha256']=sha(binary.read_bytes())
(out/'build.receipt.json').write_text(json.dumps(receipt,indent=1)+'\n');print(json.dumps(receipt),flush=True)
if r.returncode:print(log.read_text()[-4500:]);raise SystemExit(r.returncode)
