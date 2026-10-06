import os,json,pathlib,subprocess,hashlib,time
repo=pathlib.Path('/private/tmp/gomap-cow-execution-o2nauuzm/c2')
out=pathlib.Path(__file__).parent
env=dict(os.environ,GOWORK='off',PYTHONDONTWRITEBYTECODE='1')
def identity():
 files=subprocess.check_output(['git','ls-files','-z'],cwd=repo).decode().split('\0');files=[p for p in files if p]
 return {p:hashlib.sha256((repo/p).read_bytes()).hexdigest() for p in files}
pre=identity();(out/'source-pre.json').write_text(json.dumps(pre,sort_keys=True,indent=2)+'\n')
environment=subprocess.check_output(['go','env','-json','GOVERSION','GOOS','GOARCH','GOROOT','GOCACHE','GOMODCACHE','GOWORK','CGO_ENABLED'],cwd=repo,env=env)
(out/'environment.json').write_bytes(environment)
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
commands=[]
pattern='^(TestCOWPublicModeledStableImageCheckpointAndNoWALVolatileAck|TestCOWPublicModeledInterruptedDependencyAndRootPublicationCuts)$'
coordinator='^(TestRetryableFailureRetainsDebtAndFailsOnlyCapturedWaiters|TestRetryableFailureDoesNotFailWaiterAddedInFlight)$'
for mode,flags in [('normal',[]),('race',['-race']),('safe',['-tags','treedb_safe'])]:
 for target,regex,package in [('coordinator',coordinator,'./TreeDB/internal/rootpublication'),('cow-modeled',pattern,'./TreeDB')]:
  name=mode+'-'+target;args=['go','test','-json','-count=3',*flags,'-run',regex,package]
  start=time.time()
  with (out/(name+'.jsonl')).open('wb') as stdout,(out/(name+'.stderr')).open('wb') as stderr:
   result=subprocess.run(args,cwd=repo,env=env,stdout=stdout,stderr=stderr)
  commands.append(dict(name=name,args=args,cwd=str(repo),env_overrides={'GOWORK':'off','PYTHONDONTWRITEBYTECODE':'1'},exit_code=result.returncode,started_unix=start,ended_unix=time.time()))
  (out/'commands-and-exits.json').write_text(json.dumps(commands,indent=2)+'\n')
  print(name,'exit',result.returncode,flush=True)
  if result.returncode:break
 if commands[-1]['exit_code']:break
post=identity();(out/'source-post.json').write_text(json.dumps(post,sort_keys=True,indent=2)+'\n')
status=subprocess.check_output(['git','status','--porcelain','-uall'],cwd=repo,text=True)
(out/'source-proof.json').write_text(json.dumps(dict(head=head,file_count=len(pre),before_manifest_sha256=hashlib.sha256((out/'source-pre.json').read_bytes()).hexdigest(),after_manifest_sha256=hashlib.sha256((out/'source-post.json').read_bytes()).hexdigest(),changed_paths=[p for p in set(pre)|set(post) if pre.get(p)!=post.get(p)],git_status=status),indent=2)+'\n')
raise SystemExit(1 if any(c['exit_code'] for c in commands) or pre!=post else 0)
