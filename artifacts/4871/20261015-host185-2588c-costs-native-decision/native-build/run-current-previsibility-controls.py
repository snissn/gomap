from pathlib import Path
import os,subprocess,json,hashlib,time,signal,datetime
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'native';A=D/'native-build/current-previsibility-4aa7e';assert not A.exists();A.mkdir();(A/'tmp').mkdir();G=D/'toolchains/go1.26.3'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def manifest():
 return {'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=W,text=True).strip(),'files':{p:sha(W/p) for p in subprocess.check_output(['git','ls-files'],cwd=W,text=True).splitlines() if (W/p).is_file()},'status':subprocess.check_output(['git','status','--porcelain'],cwd=W,text=True)}
before=manifest();assert before['head']=='4aa7ebb23fc925a6dcc8d8abee3d2c3bfd6efe35' and not before['status'];(A/'source-before.json').write_text(json.dumps(before,indent=2)+'\n')
env=os.environ.copy();settings={'GOROOT':str(G),'GOTOOLCHAIN':'local','GOWORK':'off','GOMAXPROCS':'2','GOMEMLIMIT':'8GiB','GOGC':'100','GODEBUG':'','GOCACHE':str(D/'native-build/cache'),'GOMODCACHE':str(D/'ordinary-work/modcache'),'TMPDIR':str(A/'tmp'),'GOPROXY':'off','GOSUMDB':'off','CGO_ENABLED':'1','GOENV':'off','GOFLAGS':''};env.update(settings)
args=[str(G/'bin/go'),'test','-p=1','./TreeDB/db','./TreeDB/mvcc','-run','^(TestNativeStablePagerPartialTokenCancellationV6|TestNativeMVCCRealBackendRouteRefusalPreservesCustodyV6|TestNativeMVCCCopiedParentAcrossBothDurableSlotsV6)$','-count=1','-v','-timeout=3m']
plan={'authorization':'Root authorizes one unchanged existing previsibility safety control batch at exact recovery source. All native public turns retain originalQ32/1MiB; existing phase26 unconditional refusal prevents native builder/activation/enqueue/publication. No final native driver/Finish or qualification allowed. Ordinary fixture writes/reopen are actual. Stop on first batch failure; no automatic retry.','argv':args,'env':settings,'outer_timeout_seconds':230,'at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'compiler_sha256':sha(G/'bin/go')};(A/'command.json').write_text(json.dumps(plan,indent=2)+'\n');start=time.monotonic();terminated=False
with (A/'run.log').open('w') as out:
 p=subprocess.Popen(args,cwd=W,env=env,stdout=out,stderr=subprocess.STDOUT,start_new_session=True)
 try:code=p.wait(timeout=230)
 except subprocess.TimeoutExpired:terminated=True;os.killpg(p.pid,signal.SIGKILL);code=p.wait()
after=manifest();(A/'source-after.json').write_text(json.dumps(after,indent=2)+'\n');r={'exit':code,'elapsed_seconds':time.monotonic()-start,'terminated_process_group':terminated,'source_equal':before==after,'source_files':len(before['files']),'log_sha256':sha(A/'run.log'),'native_publication_qualification':False};(A/'result.json').write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r),flush=True);print((A/'run.log').read_text(),flush=True)
