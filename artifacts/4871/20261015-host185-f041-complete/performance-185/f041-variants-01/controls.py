import pathlib,json,subprocess,os,time
D=pathlib.Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');O=D/'performance-185/f041-variants-01';S=D/'ordinary-final';G=D/'toolchains/go1.26.3';T=pathlib.Path('/mnt/fast4tb/mvcc4871tmp');T.mkdir(exist_ok=True)
E=os.environ.copy();E.update(GOROOT=str(G),GOTOOLCHAIN='local',GOWORK='off',GOMAXPROCS='2',GOCACHE=str(D/'ordinary-work/cache'),GOMODCACHE=str(D/'ordinary-work/modcache'),TMPDIR=str(T))
plan=[('pager',[str(G/'bin/go'),'test','-p=1','-overlay',str(O/'pager-overlay.json'),'./TreeDB/pager','-count=1','-timeout=3m']),('reset',[str(G/'bin/go'),'test','-p=1','-overlay',str(O/'reset-overlay.json'),'./TreeDB/tree','-count=1','-timeout=3m']),('unix-short-tmp',[str(G/'bin/go'),'test','-p=1','./TreeDB/nativewire','-run=^TestServeUnixSocketAndDialContext$','-count=1','-timeout=90s'])];results=[]
for label,argv in plan:
 t=time.time()
 with (O/(label+'-controls.txt')).open('w') as f:r=subprocess.run(argv,cwd=S,env=E,stdout=f,stderr=subprocess.STDOUT,timeout=210)
 results.append({'label':label,'argv':argv,'exit':r.returncode,'elapsed_s':time.time()-t,'TMPDIR':str(T)});(O/'controls.json').write_text(json.dumps(results,indent=2)+'\n');print(label,r.returncode,flush=True)
 if r.returncode:raise SystemExit(r.returncode)
