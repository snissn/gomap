import os,json,subprocess,time,hashlib,signal
from pathlib import Path
r=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015'); d=r/'performance-185'; w=r/'coordinator'; gr=r/'toolchains/go1.26.3'
env=os.environ.copy();env.update(GOROOT=str(gr),GOTOOLCHAIN='local',GOWORK='off',GOPROXY='off',GOSUMDB='off',CGO_ENABLED='1',GOMAXPROCS='2',TMPDIR=str(d/'tmp'),GOCACHE=str(r/'ordinary-work/cache'),GOMODCACHE=str(r/'ordinary-work/modcache'))
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=w,text=True).strip()=='5bca87ffe6e1bfba2d693264d2dc82936adc234f'
assert not subprocess.check_output(['git','status','--porcelain'],cwd=w,text=True).strip()
for label,pkg,flags in [('baseline-db','./TreeDB/db',[]),('baseline-primary','./TreeDB/mvcc',['-tags=treedb_test','-trimpath','-buildvcs=false','-overlay='+str(d/'baseline-overlay.json')])]:
 cmd=[str(gr/'bin/go'),'test','-c','-p=1',*flags,'-o',str(d/(label+'.test')),pkg]
 rec={'argv':cmd,'cwd':str(w),'head':'5bca87ffe6e1bfba2d693264d2dc82936adc234f','env':{k:env[k] for k in ['GOROOT','GOTOOLCHAIN','GOWORK','GOPROXY','GOSUMDB','CGO_ENABLED','GOMAXPROCS','TMPDIR','GOCACHE','GOMODCACHE']}}
 t=time.monotonic()
 with (d/(label+'-build.log')).open('w') as f:
  p=subprocess.Popen(cmd,cwd=w,env=env,stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
  try: rc=p.wait(timeout=180)
  except subprocess.TimeoutExpired: os.killpg(p.pid,signal.SIGKILL);p.wait();rc=124
 rec.update(exit=rc,elapsed_s=time.monotonic()-t)
 if rc==0:rec['binary_sha256']=hashlib.sha256((d/(label+'.test')).read_bytes()).hexdigest()
 (d/(label+'-build.json')).write_text(json.dumps(rec,indent=2));print(label,rc,flush=True)
 if rc:raise SystemExit(rc)
