import os,subprocess,json,time,hashlib
from pathlib import Path
r=Path('/private/tmp/gomap-cow-execution-o2nauuzm/c2')
p=r.parent/'artifacts/c2/leaf-reclamation-windows440-repair'
env=dict(os.environ,GOWORK='off',PYTHONDONTWRITEBYTECODE='1')
def source():
 names=subprocess.check_output(['git','ls-files','-z'],cwd=r).decode().split('\0')
 return {n:hashlib.sha256((r/n).read_bytes()).hexdigest() for n in names if n and (r/n).is_file()}
pre=source(); (p/'source-pre.json').write_text(json.dumps(pre,sort_keys=True,indent=2)+'\n')
(p/'environment.json').write_text(json.dumps({'go_version':subprocess.check_output(['go','version'],cwd=r,env=env,text=True).strip(),'go_env':json.loads(subprocess.check_output(['go','env','-json','GOOS','GOARCH','GOVERSION','GOROOT','GOCACHE','GOMODCACHE','GOWORK'],cwd=r,env=env))},indent=2)+'\n')
receipts=[]
for label,flags in [('normal',[]),('race',['-race']),('safe',['-tags','treedb_safe'])]:
 cmd=['go','test','-json',*flags,'-count=3','-run','^TestCOWPublicEmptyCheckpointLeafRegistrationProgress$','./TreeDB']
 began=time.time()
 with (p/(label+'.jsonl')).open('wb') as out,(p/(label+'.stderr')).open('wb') as err:
  result=subprocess.run(cmd,cwd=r,env=env,stdout=out,stderr=err)
 receipts.append({'label':label,'command':cmd,'cwd':str(r),'env_overrides':{'GOWORK':'off','PYTHONDONTWRITEBYTECODE':'1'},'exit_code':result.returncode,'elapsed_seconds':time.time()-began})
 (p/'commands-and-exits.json').write_text(json.dumps(receipts,indent=2)+'\n')
 if result.returncode: break
post=source(); (p/'source-post.json').write_text(json.dumps(post,sort_keys=True,indent=2)+'\n')
(p/'source-proof.json').write_text(json.dumps({'tracked_working_file_count':len(pre),'changed_during_run':[n for n in pre if pre[n]!=post.get(n)],'added_during_run':sorted(set(post)-set(pre)),'removed_during_run':sorted(set(pre)-set(post)),'calibration':'Tracked working-file bytes include intentional staged parent changes; no blanket untracked/no-extra assertion.'},indent=2)+'\n')
print(json.dumps(receipts,indent=2)); print('drift',pre!=post)
