from pathlib import Path
import subprocess,hashlib,json,os,time
base=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013/sprint-performance-20261014')
repo=base.parent/'native-4878-traversal';ordinary=base.parent/'ordinary-5111'
goRoot=base.parent/'toolchains/go1.26.3-from-185-20261014'
old='28aea01e19f1d62743dec5625ec3d22c1acb6703';source=base/'sources/before-sprint'
def sha(b):return hashlib.sha256(b).hexdigest()
entries=subprocess.check_output(['git','-C',str(repo),'ls-tree','-rz',old]).split(b'\0')
rows=[]
for entry in entries:
 if not entry:continue
 meta,name=entry.split(b'\t',1);name=name.decode();mode,kind,oid=meta.decode().split()
 if kind!='blob' or not name.endswith('_test.go') or str(Path(name).parent) not in ['TreeDB','TreeDB/db','TreeDB/caching']:continue
 blob=subprocess.check_output(['git','-C',str(repo),'cat-file','blob',oid]);target=source/name
 assert not target.exists();target.write_bytes(blob);rows.append({'path':name,'blob':oid,'bytes':len(blob),'sha256':sha(blob)})
(base/'evidence/before-sprint-original-micro-test-projection.json').write_text(json.dumps({'commit':old,'test_files':rows},indent=1)+'\n')
env=dict(os.environ,GOROOT=str(goRoot),GOWORK='off',GOTOOLCHAIN='local',CGO_ENABLED='1',GOMAXPROCS='4')
for label,root,packages in [('before-sprint',source,['db','caching','treedb']),('coherent-candidate',ordinary,['caching','treedb'])]:
 for package in packages:
  path='./TreeDB'+('' if package=='treedb' else '/'+package)
  argv=[str(goRoot/'bin/go'),'test','-c','-trimpath','-buildvcs=false','-o',str(base/'binaries'/(label+'-raw-'+package+'.test')),path]
  log=base/'evidence'/(label+'-raw-'+package+'-build.log');assert not log.exists();started=time.monotonic()
  with log.open('wb') as f:r=subprocess.run(argv,cwd=root,env=env,stdout=f,stderr=subprocess.STDOUT)
  receipt={'label':label,'package':package,'argv':argv,'env':{k:env[k] for k in ['GOROOT','GOWORK','GOTOOLCHAIN','CGO_ENABLED','GOMAXPROCS']},'exit':r.returncode,'wall_seconds':time.monotonic()-started,'raw_sha256':sha(log.read_bytes())}
  if not r.returncode:receipt['binary_sha256']=sha((base/'binaries'/(label+'-raw-'+package+'.test')).read_bytes())
  log.with_suffix('.receipt.json').write_text(json.dumps(receipt,indent=1)+'\n');print(json.dumps(receipt),flush=True)
  if r.returncode:print(log.read_text()[-3000:]);raise SystemExit(r.returncode)
