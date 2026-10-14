from pathlib import Path
import hashlib,json,os,shutil,subprocess,sys,time
base=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013')
repo=base/'native-4878-traversal'
out=base/'evidence/root-primary-empty-fastpath-20261014'
go=base/'toolchains/go1.26.3-from-185-20261014'
source=repo/'TreeDB/tree/owned_iterator.go'
test=repo/'TreeDB/tree/owned_root_empty_test.go'
def sha(b):return hashlib.sha256(b).hexdigest()
def git(*a):return subprocess.check_output(['git','-C',str(repo),*a]).decode().strip()
env=dict(os.environ,GOROOT=str(go),GOWORK='off',GOTOOLCHAIN='local',CGO_ENABLED='1',GOMAXPROCS='4')
mode=sys.argv[1]
if mode=='red':
 assert not out.exists() and not test.exists()
 out.mkdir(parents=True)
 (out/'owned_iterator.before.go').write_bytes(source.read_bytes())
 (out/'baseline.json').write_text(json.dumps({'head':git('rev-parse','HEAD'),'source_sha256':sha(source.read_bytes()),'status':git('status','--porcelain=v1','-uall')},indent=1)+'\n')
 shutil.copyfile(base/'owned_root_empty_test.go',test)
 subprocess.run([str(go/'bin/gofmt'),'-w',str(test)],check=True)
 argv=[str(go/'bin/go'),'test','./TreeDB/tree','-run','^TestOwnedUserRootEmptyCapturedPrimary$/^primary_empty$','-count=1','-timeout=90s','-v']
else:
 assert mode=='green' and json.loads((out/'red.receipt.json').read_text())['exit']==1
 old=(out/'owned_iterator.before.go').read_bytes()
 assert source.read_bytes()==old
 before='''// OwnedUserRootEmpty proves emptiness from this exact captured tree root. It
// reads one validated index page without iterator or key scratch allocation.
// An external root or any nonempty/internal root conservatively returns false.'''
 after='''// OwnedUserRootEmpty proves emptiness from this exact captured tree root
// without iterator or key scratch allocation. A PRIMARY directory is empty only
// when it has no components and its digest-validated base is an empty leaf.
// External roots and nonempty/internal bases conservatively return false.'''
 text=old.decode();assert text.count(before)==1;text=text.replace(before,after)
 before='\treturn n.Type() == page.PageTypeLeaf && n.Count() == 0, nil\n'
 after='''	if n.Type() != page.PageTypePrimaryDirectory {
		return n.Type() == page.PageTypeLeaf && n.Count() == 0, nil
	}
	directory, err := node.DecodePrimaryDirectory(n.Data())
	if err != nil {
		return false, err
	}
	if directory.Count() != 0 {
		return false, nil
	}
	base, _ := directory.Base()
	if err := t.loadChildRefViewInto(&n, base.Ref, true, false); err != nil {
		return false, err
	}
	if !node.VerifyPrimaryOperand(base, n.Data()) || (n.Type() != page.PageTypeLeaf && n.Type() != page.PageTypeInternal) {
		return false, node.ErrPrimaryDirectory
	}
	return n.Type() == page.PageTypeLeaf && n.Count() == 0, nil
'''
 assert text.count(before)==1;text=text.replace(before,after)
 source.write_text(text)
 subprocess.run([str(go/'bin/gofmt'),'-w',str(source)],check=True)
 (out/'owned_iterator.after.go').write_bytes(source.read_bytes())
 (out/'owned_root_empty_test.go').write_bytes(test.read_bytes())
 argv=[str(go/'bin/go'),'test','./TreeDB/tree','-run','^TestOwnedUserRootEmptyCapturedPrimary$','-count=1','-timeout=90s','-v']
log=out/(mode+'.log');assert not log.exists()
started=time.time()
with log.open('wb') as f:r=subprocess.run(argv,cwd=repo,env=env,stdout=f,stderr=subprocess.STDOUT,timeout=150)
receipt={'argv':argv,'env':{k:env[k] for k in ['GOROOT','GOWORK','GOTOOLCHAIN','CGO_ENABLED','GOMAXPROCS']},'exit':r.returncode,'wall_seconds':time.time()-started,'raw_sha256':sha(log.read_bytes()),'source_sha256':sha(source.read_bytes()),'test_sha256':sha(test.read_bytes()),'head':git('rev-parse','HEAD')}
(out/(mode+'.receipt.json')).write_text(json.dumps(receipt,indent=1)+'\n')
print(json.dumps(receipt));print(log.read_text()[-5500:])
assert r.returncode==(1 if mode=='red' else 0)
