import pathlib,subprocess,os,time,json,hashlib
D=pathlib.Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');O=D/'performance-185/f041-variants-01';G=D/'toolchains/go1.26.3'
E=os.environ.copy();E.update(GOROOT=str(G),GOTOOLCHAIN='local',GOWORK='off',GOMAXPROCS='2',GOCACHE=str(D/'ordinary-work/cache'),GOMODCACHE=str(D/'ordinary-work/modcache'),TMPDIR='/mnt/fast4tb/mvcc4871tmp')
subprocess.run([str(G/'bin/gofmt'),'-w',str(O/'pools-clear.go')],check=True);results=[]
for variant,src in [('baseline','coordinator'),('combined','ordinary-final')]:
 for pkg,path in [('db','./TreeDB/db'),('treedb','./TreeDB')]:
  b=O/(variant+'-'+pkg+'.test');argv=[str(G/'bin/go'),'test','-c','-p=1','-trimpath','-buildvcs=false','-o',str(b)]
  if variant=='combined':argv+=['-overlay',str(O/'combined-overlay.json')]
  argv+=[path];t=time.time()
  with (O/(variant+'-'+pkg+'-build.log')).open('w') as f:r=subprocess.run(argv,cwd=D/src,env=E,stdout=f,stderr=subprocess.STDOUT,timeout=180)
  results.append({'label':variant+'-'+pkg,'argv':argv,'cwd':str(D/src),'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=D/src,text=True).strip(),'exit':r.returncode,'sha256':hashlib.sha256(b.read_bytes()).hexdigest() if b.exists() else None,'elapsed_s':time.time()-t});(O/'combined-builds.json').write_text(json.dumps(results,indent=2)+'\n');print(variant,pkg,r.returncode,flush=True)
  if r.returncode:raise SystemExit(r.returncode)
argv=[str(O/'combined-db.test'),'-test.run=Test(Snapshot.*|OneShot.*|NativeMVCCSnapshot.*|Primary.*Snapshot.*)$','-test.count=1','-test.timeout=90s'];t=time.time()
with (O/'combined-controls.txt').open('w') as f:r=subprocess.run(argv,cwd=O,env=E,stdout=f,stderr=subprocess.STDOUT,timeout=100)
(O/'combined-controls.json').write_text(json.dumps({'argv':argv,'exit':r.returncode,'elapsed_s':time.time()-t},indent=2)+'\n');print('combined controls',r.returncode,flush=True)
