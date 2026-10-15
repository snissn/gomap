import pathlib,os,subprocess,json,hashlib,time
D=pathlib.Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');S=D/'ordinary-final';O=D/'performance-185/f041-variants-01';G=D/'toolchains/go1.26.3'
E=os.environ.copy();E.update(GOROOT=str(G),GOTOOLCHAIN='local',GOWORK='off',GOMAXPROCS='2',GOCACHE=str(D/'ordinary-work/cache'),GOMODCACHE=str(D/'ordinary-work/modcache'),TMPDIR=str(D/'ordinary-work/tmp'))
for f in ['tree-reset.go','pager-get.go']:subprocess.run([str(G/'bin/gofmt'),'-w',str(O/f)],check=True)
results=[]
for variant,pkg in [('control','db'),('control','treedb'),('reset','db'),('pager','db'),('pager','treedb')]:
 out=O/(variant+'-'+pkg+'.test');cmd=[str(G/'bin/go'),'test','-c','-p=1','-trimpath','-buildvcs=false','-o',str(out)]
 if variant!='control':cmd+=['-overlay',str(O/(variant+'-overlay.json'))]
 cmd+=['./TreeDB/db' if pkg=='db' else './TreeDB']
 t=time.time()
 with (O/(variant+'-'+pkg+'-build.log')).open('w') as f:r=subprocess.run(cmd,cwd=S,env=E,stdout=f,stderr=subprocess.STDOUT,timeout=180)
 rec={'variant':variant,'package':pkg,'argv':cmd,'exit':r.returncode,'elapsed_s':time.time()-t,'sha256':hashlib.sha256(out.read_bytes()).hexdigest() if out.exists() else None};results.append(rec)
 (O/'builds.json').write_text(json.dumps({'source':'f0415773f13a522dea5c1debdaecba09d15585dd','environment':{k:E[k] for k in ['GOROOT','GOTOOLCHAIN','GOWORK','GOMAXPROCS','GOCACHE','GOMODCACHE','TMPDIR']},'overlays':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in O.glob('*.go')},'builds':results},indent=2)+'\n');print(variant,pkg,r.returncode,flush=True)
 if r.returncode:raise SystemExit(r.returncode)
