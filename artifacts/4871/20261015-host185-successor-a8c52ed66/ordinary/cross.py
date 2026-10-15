import sys,os,json,subprocess,hashlib,time,pathlib,signal
root=pathlib.Path('/mnt/fast4tb/gomap-mvcc-4871-20261015'); work=root/'ordinary-repair'; out=root/'ordinary-repair-work/logs'/sys.argv[1]; out.mkdir()
env=os.environ.copy(); env.update(GOROOT=str(root/'toolchains/go1.26.3'),GOWORK='off',GOTOOLCHAIN='local',GOMAXPROCS='2',TMPDIR=str(root/'ordinary-repair-work/tmp'),GOCACHE=str(root/'ordinary-work/cache'),GOMODCACHE=str(root/'ordinary-work/modcache'),GOPROXY='https://proxy.golang.org,direct',GOSUMDB='sum.golang.org',CGO_ENABLED='0',GOOS='windows',GOARCH='amd64')
def identity():
 files=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard'],cwd=work,text=True).splitlines()
 return {'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=work,text=True).strip(),'files':{f:hashlib.sha256((work/f).read_bytes()).hexdigest() for f in files if (work/f).is_file()}}
argv=[str(root/'toolchains/go1.26.3/bin/go')]+sys.argv[2:]
(out/'before.json').write_text(json.dumps(identity(),sort_keys=True));(out/'command.json').write_text(json.dumps({'argv':argv,'cwd':str(work),'env':{k:env[k] for k in ['GOROOT','GOWORK','GOTOOLCHAIN','GOMAXPROCS','TMPDIR','GOCACHE','GOMODCACHE','GOPROXY','GOSUMDB','CGO_ENABLED','GOOS','GOARCH']}}))
start=time.monotonic()
with (out/'raw.txt').open('w') as log:
 process=subprocess.Popen(argv,cwd=work,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 try: code=process.wait(timeout=180)
 except subprocess.TimeoutExpired:
  os.killpg(process.pid,signal.SIGTERM)
  try: process.wait(timeout=15)
  except subprocess.TimeoutExpired: os.killpg(process.pid,signal.SIGKILL); process.wait()
  code=124
(out/'result.json').write_text(json.dumps({'exit':code,'elapsed':time.monotonic()-start}));(out/'after.json').write_text(json.dumps(identity(),sort_keys=True));print(out, 'exit',code,flush=True)
print((out/'raw.txt').read_text()[-10000:])
