import hashlib, importlib.util, json, os, pathlib, shutil, signal, subprocess, time
D=pathlib.Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
W=D/'m0-observation-harness'; A=D/'m0-observation-preparation'/'compile-only'; P=D/'ordinary-repair'; G=D/'toolchains/go1.26.9'
A.mkdir()
spec=importlib.util.spec_from_file_location('m0',W/'.github/scripts/mvcc_m0_observation.py'); h=importlib.util.module_from_spec(spec);spec.loader.exec_module(h)
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=P,text=True).strip()==h.CANDIDATE
assert not subprocess.check_output(['git','status','--porcelain'],cwd=P)
assert h.digest(G/'bin/go')==h.GO_SHA
before=h.manifest(P);h.save(A/'source-before.json',before)
for name in ['tmp','cache']: (A/name).mkdir()
shutil.copytree(D/'ordinary-work/modcache', A/'modcache', symlinks=True)
env=dict(os.environ,GOROOT=str(G),GOTOOLCHAIN='local',GOWORK='off',GOENV='off',GOFLAGS='',GOMAXPROCS='2',GOMEMLIMIT='8GiB',GOGC='100',GODEBUG='',GOPROXY='off',GOSUMDB='off',CGO_ENABLED='1',GOOS='linux',GOARCH='amd64',GOAMD64='v1',GOEXPERIMENT='',TMPDIR=str(A/'tmp'),GOCACHE=str(A/'cache'),GOMODCACHE=str(A/'modcache'))
h.save(A/'environment.json',{k:env[k] for k in ['GOROOT','GOTOOLCHAIN','GOWORK','GOENV','GOFLAGS','GOMAXPROCS','GOMEMLIMIT','GOGC','GODEBUG','GOPROXY','GOSUMDB','CGO_ENABLED','GOOS','GOARCH','GOAMD64','GOEXPERIMENT','TMPDIR','GOCACHE','GOMODCACHE']})
source=A/'vacuum_collection_bench_test.go';source.write_bytes(h.checked_overlay((P/h.BENCH_SOURCE).read_bytes()))
overlay=A/'overlay.json';h.save(overlay,{'Replace':{str(P/h.BENCH_SOURCE):str(source),str(P/'TreeDB/db/mvcc_m0_observation_overlay_test.go'):str(W/'.github/scripts/mvcc_m0_observation_gc.go.txt')}})
receipts=[]
try:
 for arm,flags in [('pristine',[]),('overlay',['-overlay',str(overlay)])]:
  binary=A/(arm+'.test')
  for label,argv in [(arm+'-compile',[str(G/'bin/go'),'test','-p=1',*flags,'-c','-o',str(binary),'./TreeDB/db']),(arm+'-buildinfo',[str(G/'bin/go'),'version','-m',str(binary)])]:
   started=time.monotonic();output=A/(label+'.txt');limit=180
   with output.open('wb') as stream:
    process=subprocess.Popen(argv,cwd=P,env=env,stdout=stream,stderr=subprocess.STDOUT,start_new_session=True)
    try: status=process.wait(timeout=limit)
    except BaseException:
     os.killpg(process.pid,signal.SIGKILL);process.wait();raise
   receipt={'label':label,'argv':argv,'cwd':str(P),'exit':status,'elapsed_s':time.monotonic()-started,'outer_timeout_s':limit,'output_sha256':h.digest(output)}
   receipts.append(receipt);h.save(A/'commands.json',receipts)
   if status: raise RuntimeError(label+' failed')
  h.save(A/(arm+'-binary.json'),{'sha256':h.digest(binary),'bytes':binary.stat().st_size})
finally:
 after=h.manifest(P);h.save(A/'source-after.json',after);h.save(A/'source-equality.json',{'equal':before==after});assert before==after
